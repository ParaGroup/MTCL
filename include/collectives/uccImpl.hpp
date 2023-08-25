#ifndef UCCCOLLIMPL_HPP
#define UCCCOLLIMPL_HPP

#include "collectiveImpl.hpp"
#include <ucc/api/ucc.h>

#define STR(x) #x
#define UCC_CHECK(_call)                                                \
    if (UCC_OK != (_call)) {                                            \
        MTCL_ERROR("[internal]:\t", "UCC call failed %s\n", STR(_call)); \
    }

class UCCCollective : public CollectiveImpl {

typedef struct UCC_coll_info {
    std::vector<Handle*>* handles;      // Vector of handles of participants
    int rank;                           // Local rank
    int size;                           // Team size
    bool root;
    UCCCollective* coll_obj;
} UCC_coll_info_t;

protected:
    int rank, size;
    bool root;
    ucc_lib_config_h     lib_config;
    ucc_context_config_h ctx_config;
    ucc_team_h           team;
    ucc_context_h        ctx;
    ucc_lib_h            lib;
    ucc_coll_req_h req = nullptr;
    ssize_t last_probe = -1;
    bool closing = false;

    static ucc_status_t oob_allgather(void *sbuf, void *rbuf, size_t msglen,
                                  void *coll_info, void **req) {
        UCC_coll_info_t* info = (UCC_coll_info_t*)coll_info;

        auto handles = info->handles;

        if(info->root) {
            for(int i = 0; i < info->size-1; i++) {
                handles->at(i)->send(&info->rank, sizeof(int));
                int remote_rank;
                info->coll_obj->receiveFromHandle(handles->at(i), &remote_rank, sizeof(int));
                info->coll_obj->receiveFromHandle(handles->at(i), (char*)rbuf+(remote_rank*msglen), msglen);
            }

            memcpy((char*)rbuf+(info->rank*msglen), sbuf, msglen);
            for(auto& p : *handles) {
                p->send(rbuf, msglen*(info->size));
            }

        }
        else {
            int root_rank;
            info->coll_obj->receiveFromHandle(handles->at(0), &root_rank, sizeof(int));

            info->coll_obj->root_rank = root_rank;
            handles->at(0)->send(&info->rank, sizeof(int));
            handles->at(0)->send(sbuf, msglen);
            size_t sz;
            info->coll_obj->probeHandle(handles->at(0), sz, true);
            info->coll_obj->receiveFromHandle(handles->at(0), rbuf, sz);
        }

        return UCC_OK;
    }

    static ucc_status_t oob_allgather_test(void *req) {
        return UCC_OK;
    }

    static ucc_status_t oob_allgather_free(void *req) {
        return UCC_OK;
    }


    /* Creates UCC team from a group of handles. */
    static ucc_team_h create_ucc_team(UCC_coll_info_t* info, ucc_context_h ctx) {
        int rank = info->rank;
        int size = info->size;
        ucc_team_h        team;
        ucc_team_params_t team_params;
        ucc_status_t      status;

        team_params.mask          = UCC_TEAM_PARAM_FIELD_OOB;
        team_params.oob.allgather = oob_allgather;
        team_params.oob.req_test  = oob_allgather_test;
        team_params.oob.req_free  = oob_allgather_free;
        team_params.oob.coll_info = (void*)info;
        team_params.oob.n_oob_eps = size;
        team_params.oob.oob_ep    = rank;

        UCC_CHECK(ucc_team_create_post(&ctx, 1, &team_params, &team));
        while (UCC_INPROGRESS == (status = ucc_team_create_test(team))) {
            UCC_CHECK(ucc_context_progress(ctx));
        };
        if (UCC_OK != status) {
            fprintf(stderr, "failed to create ucc team\n");
            //TODO: CHECK
            exit(1);
        }
        return team;
    }

public:
    int root_rank;
	                                                 
    UCCCollective(std::vector<Handle*> participants, int size, bool root, int rank, int uniqtag)
		: CollectiveImpl(participants, size, rank, uniqtag), root(root) {
        /* === UCC collective operation === */
        /* Init ucc library */
        ucc_lib_params_t lib_params = {
            .mask        = UCC_LIB_PARAM_FIELD_THREAD_MODE,
            .thread_mode = UCC_THREAD_SINGLE
        };
        UCC_CHECK(ucc_lib_config_read(NULL, NULL, &lib_config));
        UCC_CHECK(ucc_init(&lib_params, lib_config, &lib));
        ucc_lib_config_release(lib_config);

        if(root) root_rank = rank;
        this->rank = rank; 

        UCC_coll_info_t* info = new UCC_coll_info_t();
        info->handles = &participants;
        info->rank = CollectiveImpl::rank;
        info->size = CollectiveImpl::nparticipants;
        info->root = root;
        info->coll_obj = this;

        /* Init ucc context for a specified UCC_TEST_TLS */
        ucc_context_oob_coll_t oob = {
            .allgather    = oob_allgather,
            .req_test     = oob_allgather_test,
            .req_free     = oob_allgather_free,
            .coll_info    = (void*)info,
            .n_oob_eps    = (uint32_t)(CollectiveImpl::nparticipants),
            .oob_ep       = (uint32_t)(CollectiveImpl::rank) 
        };


        ucc_context_params_t ctx_params = {
            .mask             = UCC_CONTEXT_PARAM_FIELD_OOB,
            .oob              = oob
        };

        UCC_CHECK(ucc_context_config_read(lib, NULL, &ctx_config));
        UCC_CHECK(ucc_context_create(lib, &ctx_params, ctx_config, &ctx));
        ucc_context_config_release(ctx_config);

        team = create_ucc_team(info, ctx);
    }

    // UCX needs to override basic peek in order to correctly catch messages
    // using UCX collectives
    bool peek() override {
        size_t sz;
        ssize_t res = this->probe(sz, false);

        return res > 0;
    }

};


class BroadcastUCC : public UCCCollective {

public:
    BroadcastUCC(std::vector<Handle*> participants, int size, bool root, int rank, int uniqtag) : UCCCollective(participants, size, root, rank, uniqtag) {}


    ssize_t probe(size_t& size, const bool blocking=true)  {
		MTCL_ERROR("[internal]:\t", "Broadcast::probe operation not supported\n");
		errno=EINVAL;
		return -1;
	}
	// TODO: error CHECK!
    ssize_t send(const void* buff, size_t size) {
        ucc_coll_args_t      args;
        ucc_coll_req_h       request;

        args.mask              = 0;
        args.coll_type         = UCC_COLL_TYPE_BCAST;
        args.src.info.mem_type = UCC_MEMORY_TYPE_HOST;
        args.root              = root_rank;
				
        /* BROADCAST DATA */
        args.src.info.buffer = (void*)buff;
        args.src.info.count = size;
        args.src.info.datatype = UCC_DT_UINT8;

        UCC_CHECK(ucc_collective_init(&args, &request, team)); 
        UCC_CHECK(ucc_collective_post(request));    
        while (UCC_INPROGRESS == ucc_collective_test(request)) { 
            UCC_CHECK(ucc_context_progress(ctx));
        }
        ucc_collective_finalize(request);

        return size;
    }

	// TODO: error CHECK!
    ssize_t receive(void* buff, size_t size) {
        ucc_coll_args_t      args;
        ucc_coll_req_h       req;

        /* BROADCAST DATA */
        args.mask              = 0;
        args.coll_type         = UCC_COLL_TYPE_BCAST;
        args.src.info.buffer   = (void*)buff;
        args.src.info.count    = size;
        args.src.info.datatype = UCC_DT_UINT8;
        args.src.info.mem_type = UCC_MEMORY_TYPE_HOST;
        args.root              = root_rank;

        UCC_CHECK(ucc_collective_init(&args, &req, team)); 
        UCC_CHECK(ucc_collective_post(req));    
        while (UCC_INPROGRESS == ucc_collective_test(req)) { 
            UCC_CHECK(ucc_context_progress(ctx));
        }
        ucc_collective_finalize(req);
		
        return size;
    }

    ssize_t sendrecv(const void* sendbuff, size_t sendsize, void* recvbuff, size_t recvsize, size_t datasize = 1) {
		ssize_t sz=-1;
        if(root) {
            sz=this->send(sendbuff, sendsize);
			if (sz>0 && recvbuff) 
				memcpy(recvbuff, sendbuff, sendsize);			
        }
        else {
            sz=this->receive(recvbuff, recvsize);
        }
		return sz;
    }

    void close(bool close_wr=true, bool close_rd=true) {
		closing = true;
        return;
    }

    void finalize(bool, std::string name="") {
		if(!closing)
			this->close(true, true);
    }


};

class ScatterUCC : public UCCCollective {

public:
    ScatterUCC(std::vector<Handle*> participants, int size, bool root, int rank, int uniqtag) : UCCCollective(participants, size, root, rank, uniqtag) {}

    ssize_t probe(size_t& size, const bool blocking=true) {
		MTCL_ERROR("[internal]:\t", "Scatter::probe operation not supported\n");
		errno=EINVAL;
        return -1;
    }

    ssize_t send(const void* buff, size_t size) {
		MTCL_ERROR("[internal]:\t", "Scatter::send operation not supported, you must use the sendrecv method\n");
		errno=EINVAL;
        return -1;
	}

    ssize_t receive(void* buff, size_t size) {        
		MTCL_ERROR("[internal]:\t", "Gather::receive operation not supported, you must use the sendrecv method\n");
		errno=EINVAL;
        return -1;
    }

    ssize_t sendrecv(const void* sendbuff, size_t sendsize, void* recvbuff, size_t recvsize, size_t datasize = 1) {
        MTCL_UCX_PRINT(100, "sendrecv, sendsize=%ld, recvsize=%ld, datasize=%ld, nparticipants=%ld\n", sendsize, recvsize, datasize, nparticipants);

        if (sendsize == 0)
			MTCL_MPI_PRINT(0, "[internal]:\t Scatter::sendrecv \"sendsize\" is equal to zero, , this is an ERROR!\n");

        if (sendsize % datasize != 0) {
            errno = EINVAL;
            return -1;
        }

        int datacount = sendsize / datasize;

        uint32_t *sendcounts = new uint32_t[nparticipants];
        uint32_t *displs = new uint32_t[nparticipants];
        
        int displ = 0;

        int sendcount = (datacount / nparticipants) * datasize;
        int rcount = datacount % nparticipants;
            
        for (size_t i = 0; i < nparticipants; i++) {
            sendcounts[i] = sendcount;
                
            if (rcount > 0) {
                sendcounts[i] += datasize;
                rcount--;
            }
                
            displs[i] = displ;
            displ += sendcounts[i];
        }

        if ((size_t)sendcounts[rank] > recvsize) {
            MTCL_ERROR("[internal]:\t","receive buffer too small %ld instead of %ld\n", recvsize, sendcounts[rank]);
            errno = EINVAL;
            return -1;
        }

        ucc_coll_args_t args;
        ucc_coll_req_h  request;

        args.mask              = 0;
        args.coll_type         = UCC_COLL_TYPE_SCATTERV;
        args.dst.info.buffer   = (void*)recvbuff;
        args.dst.info.count    = sendcounts[rank];
        args.dst.info.datatype = UCC_DT_UINT8;
        args.dst.info.mem_type = UCC_MEMORY_TYPE_HOST;

        if(root) {
            args.src.info_v.buffer        = (void*)sendbuff;
            args.src.info_v.counts        = (ucc_count_t*)sendcounts;
            args.src.info_v.displacements = (ucc_aint_t*)displs;
            args.src.info_v.datatype      = UCC_DT_UINT8;
            args.src.info_v.mem_type      = UCC_MEMORY_TYPE_HOST;
        }

        args.root = root_rank;

        UCC_CHECK(ucc_collective_init(&args, &request, team)); 
        UCC_CHECK(ucc_collective_post(request));  

        while (UCC_INPROGRESS == ucc_collective_test(request)) { 
            UCC_CHECK(ucc_context_progress(ctx));
        }

        ucc_collective_finalize(request);
		
        recvsize = sendcounts[rank];

        delete [] sendcounts;
        delete [] displs;

        return recvsize;
    }

    void close(bool close_wr=true, bool close_rd=true) {
		closing = true;
    }

    void finalize(bool, std::string name="") {
		if (!closing)
			this->close(true, true);
    }
};

class GatherUCC : public UCCCollective {

public:
    GatherUCC(std::vector<Handle*> participants, int size, bool root, int rank, int uniqtag) : UCCCollective(participants, size, root, rank, uniqtag) {}

    ssize_t probe(size_t& size, const bool blocking=true) {
		MTCL_ERROR("[internal]:\t", "Gather::probe operation not supported\n");
		errno=EINVAL;
        return -1;
    }

    ssize_t send(const void* buff, size_t size) {
		MTCL_ERROR("[internal]:\t", "Gather::send operation not supported, you must use the sendrecv method\n");
		errno=EINVAL;
        return -1;
	}

    ssize_t receive(void* buff, size_t size) {        
		MTCL_ERROR("[internal]:\t", "Gather::receive operation not supported, you must use the sendrecv method\n");
		errno=EINVAL;
        return -1;
    }

    ssize_t sendrecv(const void* sendbuff, size_t sendsize, void* recvbuff, size_t recvsize, size_t datasize = 1) {
        MTCL_UCX_PRINT(100, "sendrecv, sendsize=%ld, recvsize=%ld, datasize=%ld, nparticipants=%ld\n", sendsize, recvsize, datasize, nparticipants);

        if (recvsize == 0)
			MTCL_ERROR("[internal]:\t", "Gather::sendrecv \"recvsize\" is equal to zero, this is an ERROR!\n");

        if (recvsize % datasize != 0) {
            errno = EINVAL;
            return -1;
        }

        int datacount = recvsize / datasize;

        uint32_t *recvcounts = new uint32_t[nparticipants];
        uint32_t *displs = new uint32_t[nparticipants];
        
        int displ = 0;

        int recvcount = (datacount / nparticipants) * datasize;
        int rcount = datacount % nparticipants;
            
        for (size_t i = 0; i < nparticipants; i++) {
            recvcounts[i] = recvcount;
                
            if (rcount > 0) {
                recvcounts[i] += datasize;
                rcount--;
            }
                
            displs[i] = displ;
            displ += recvcounts[i];
        }

        if ((size_t)recvcounts[rank] > sendsize) {
            MTCL_ERROR("[internal]:\t","sending buffer too small %ld instead of %ld\n", sendsize, recvcounts[rank]);
            errno = EINVAL;
            return -1;
        }

        ucc_coll_args_t args;
        ucc_coll_req_h  request;

        args.mask              = 0;
        args.coll_type         = UCC_COLL_TYPE_GATHERV;
        args.src.info.buffer   = (void*)sendbuff;
        args.src.info.count    = recvcounts[rank];
        args.src.info.datatype = UCC_DT_UINT8;
        args.src.info.mem_type = UCC_MEMORY_TYPE_HOST;

        if(root) {
            args.dst.info_v.buffer        = (void*)recvbuff;
            args.dst.info_v.counts        = (ucc_count_t*)recvcounts;
            args.dst.info_v.displacements = (ucc_aint_t*)displs;
            args.dst.info_v.datatype      = UCC_DT_UINT8;
            args.dst.info_v.mem_type      = UCC_MEMORY_TYPE_HOST;
        }

        args.root = root_rank;

        UCC_CHECK(ucc_collective_init(&args, &request, team)); 
        UCC_CHECK(ucc_collective_post(request));  

        while (UCC_INPROGRESS == ucc_collective_test(request)) { 
            UCC_CHECK(ucc_context_progress(ctx));
        }

        ucc_collective_finalize(request);
		
        sendsize = recvcounts[rank];

        delete [] recvcounts;
        delete [] displs;

        return sendsize;
    }

    void close(bool close_wr=true, bool close_rd=true) {
		closing = true;
    }

    void finalize(bool, std::string name="") {
		if (!closing)
			this->close(true, true);
    }

};

class AllGatherUCC : public UCCCollective {

public:
    AllGatherUCC(std::vector<Handle*> participants, int size, bool root, int rank, int uniqtag) : UCCCollective(participants, size, root, rank, uniqtag) {}

    ssize_t probe(size_t& size, const bool blocking=true) {
		MTCL_ERROR("[internal]:\t", "AllGather::probe operation not supported\n");
		errno=EINVAL;
        return -1;
    }

    ssize_t send(const void* buff, size_t size) {
		MTCL_ERROR("[internal]:\t", "AllGather::send operation not supported, you must use the sendrecv method\n");
		errno=EINVAL;
        return -1;
	}

    ssize_t receive(void* buff, size_t size) {        
		MTCL_ERROR("[internal]:\t", "AllGather::receive operation not supported, you must use the sendrecv method\n");
		errno=EINVAL;
        return -1;
    }

    ssize_t sendrecv(const void* sendbuff, size_t sendsize, void* recvbuff, size_t recvsize, size_t datasize = 1) {
        MTCL_UCX_PRINT(100, "sendrecv, sendsize=%ld, recvsize=%ld, datasize=%ld, nparticipants=%ld\n", sendsize, recvsize, datasize, nparticipants);

        if (recvsize == 0)
			MTCL_ERROR("[internal]:\t", "AllGather::sendrecv \"recvsize\" is equal to zero, this is an ERROR!\n");

        if (recvsize % datasize != 0) {
            errno = EINVAL;
            return -1;
        }

        int datacount = recvsize / datasize;

        uint32_t *recvcounts = new uint32_t[nparticipants];
        uint32_t *displs = new uint32_t[nparticipants];
        
        int displ = 0;

        int recvcount = (datacount / nparticipants) * datasize;
        int rcount = datacount % nparticipants;
            
        for (size_t i = 0; i < nparticipants; i++) {
            recvcounts[i] = recvcount;
                
            if (rcount > 0) {
                recvcounts[i] += datasize;
                rcount--;
            }
                
            displs[i] = displ;
            displ += recvcounts[i];
        }

        if ((size_t)recvcounts[rank] > sendsize) {
            MTCL_ERROR("[internal]:\t","sending buffer too small %ld instead of %ld\n", sendsize, recvcounts[rank]);
            errno = EINVAL;
            return -1;
        }

        ucc_coll_args_t args;
        ucc_coll_req_h  request;

        args.mask              = 0;
        args.coll_type         = UCC_COLL_TYPE_ALLGATHERV;
        args.src.info.buffer   = (void*)sendbuff;
        args.src.info.count    = recvcounts[rank];
        args.src.info.datatype = UCC_DT_UINT8;
        args.src.info.mem_type = UCC_MEMORY_TYPE_HOST;

        args.dst.info_v.buffer        = (void*)recvbuff;
        args.dst.info_v.counts        = (ucc_count_t*)recvcounts;
        args.dst.info_v.displacements = (ucc_aint_t*)displs;
        args.dst.info_v.datatype      = UCC_DT_UINT8;
        args.dst.info_v.mem_type      = UCC_MEMORY_TYPE_HOST;

        UCC_CHECK(ucc_collective_init(&args, &request, team)); 
        UCC_CHECK(ucc_collective_post(request));  

        while (UCC_INPROGRESS == ucc_collective_test(request)) { 
            UCC_CHECK(ucc_context_progress(ctx));
        }

        ucc_collective_finalize(request);
		
        sendsize = recvcounts[rank];

        delete [] recvcounts;
        delete [] displs;

        return sendsize;
    }

    void close(bool close_wr=true, bool close_rd=true) {
		closing = true;
    }

    void finalize(bool, std::string name="") {
		if (!closing)
			this->close(true, true);
    }

};

#endif //UCCCOLLIMPL_HPP
