#ifndef MPICOLLIMPL_HPP
#define MPICOLLIMPL_HPP

#include "collectiveImpl.hpp"
#include <mpi.h>

/**
 * @brief MPI implementation of collective operations. Abstract class, only provides
 * generic functionalities for collectives using the MPI transport. Subclasses must
 * implement collective-specific behavior.
 * 
 */
class MPICollective : public CollectiveImpl {
protected:
    bool root;
    int root_rank, local_rank;
    MPI_Comm comm;
    MPI_Group group;
    
    MPI_Request request_header = MPI_REQUEST_NULL;
    bool closing = false;
    ssize_t last_probe = -1;
    int* ranks;

public:
    MPICollective(std::vector<Handle*> participants, bool root, int uniqtag) : CollectiveImpl(participants, uniqtag), root(root) {

        //TODO: add endianess conversion
        MPI_Comm_rank(MPI_COMM_WORLD, &local_rank);
        int coll_size;
        if(root) {
            coll_size = participants.size() + 1;
            root_rank = local_rank;
            ranks = new int[participants.size()+1];
            ranks[0] = root_rank;

            for(size_t i = 0; i < participants.size(); i++) {
                participants.at(i)->send(&root_rank, sizeof(int));
                int remote_rank;
                receiveFromHandle(participants.at(i), &remote_rank, sizeof(int));
                ranks[i+1] = remote_rank;
            }

            for(auto& p : participants) {
                p->send(ranks, sizeof(int)*(participants.size()+1));  // checks!!!
            }

        }
        else {
            int remote_rank;
            receiveFromHandle(participants.at(0), &remote_rank, sizeof(int));
            root_rank = remote_rank;

            participants.at(0)->send(&local_rank, sizeof(int));
            size_t sz;
            probeHandle(participants.at(0), sz, true);
            coll_size = sz/sizeof(int);
            ranks = new int[sz];
            receiveFromHandle(participants.at(0), ranks, sz);
        }

        MPI_Group group_world;
        if (MPI_Comm_group(MPI_COMM_WORLD, &group_world) != MPI_SUCCESS) {
			MTCL_ERROR("[internal]:\t", "MPI_Collective::MPI_Comm_group\n");
		}
        if (MPI_Group_incl(group_world, coll_size, ranks, &group) != MPI_SUCCESS) {
			MTCL_ERROR("[internal]:\t", "MPI_Collective::MPI_Group_incl\n");
		}
        if (MPI_Comm_create_group(MPI_COMM_WORLD, group, uniqtag, &comm) != MPI_SUCCESS) {
			MTCL_ERROR("[internal]:\t", "MPI_Collective::MPI_Comm_create_group\n");
		}

        delete[] ranks;
        //TODO: closing connections???
    }

    // MPI needs to override basic peek in order to correctly catch messages
    // using MPI collectives
    //NOTE: if yield is disabled, this function will never be called
    bool peek() override {
        size_t sz;
        ssize_t res = this->probe(sz, false);

        return res > 0;
    }

};


class BroadcastMPI : public MPICollective {
private:

public:
    BroadcastMPI(std::vector<Handle*> participants, bool root, int uniqtag) : MPICollective(participants, root, uniqtag) {}


    ssize_t probe(size_t& size, const bool blocking=true) {
		MTCL_ERROR("[internal]:\t", "Broadcast::probe operation not supported\n");
		errno=EINVAL;
        return -1;
    }

    ssize_t send(const void* buff, size_t size) {
        MTCL_ERROR("[internal]:\t", "Broadcast::send operation not supported, you must use the sendrecv method\n");
		errno=EINVAL;
        return -1;
    }

    ssize_t receive(void* buff, size_t size) {
        MTCL_ERROR("[internal]:\t", "Broadcast::receive operation not supported, you must use the sendrecv method\n");
		errno=EINVAL;
        return -1;
    }

    ssize_t sendrecv(const void* sendbuff, size_t sendsize, void* recvbuff, size_t recvsize, size_t datasize = 1) {
        if(root) {
            if(MPI_Bcast((void*)sendbuff, sendsize, MPI_BYTE, root_rank, comm) != MPI_SUCCESS) {
                errno = ECOMM;
                return -1;
            }
            
            return sendsize;
        }
        else {
            if(MPI_Bcast((void*)recvbuff, recvsize, MPI_BYTE, root_rank, comm) != MPI_SUCCESS) {
                errno = ECOMM;
                return -1;
            }
        
            return recvsize;
        }
    }

    void close(bool close_wr=true, bool close_rd=true) {
		closing = true;		
    }

    void finalize(bool, std::string name="") {
		if (!closing)
			this->close(true,true);
		
        MPI_Group_free(&group);
        MPI_Comm_free(&comm);
    }
};

class ScatterMPI : public MPICollective {
private:

public:
    ScatterMPI(std::vector<Handle*> participants, bool root, int uniqtag) : MPICollective(participants, root, uniqtag) {}

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
        MTCL_ERROR("[internal]:\t", "Scatter::receive operation not supported, you must use the sendrecv method\n");
		errno=EINVAL;
        return -1;
    }

    ssize_t sendrecv(const void* sendbuff, size_t sendsize, void* recvbuff, size_t recvsize, size_t datasize = 1) {
        if (sendsize == 0) MTCL_MPI_PRINT(0, "[internal]:\t Scatter::sendrecv \"sendsize\" is equal to zero!\n");

        if (sendsize % datasize != 0) {
            errno = EINVAL;
            return -1;
        }

        int nparticipants;
        MPI_Comm_size(comm, &nparticipants);

        size_t datacount = sendsize / datasize;

        int *sendcounts = new int[nparticipants]();
        int *displs = new int[nparticipants]();
        
        int displ = 0;

        int sendcount = (datacount / nparticipants) * datasize;
        int rcount = datacount % nparticipants;
            
        for (int i = 0; i < nparticipants; i++) {
            sendcounts[i] = sendcount;
                
            if (rcount > 0) {
                sendcounts[i] += datasize;
                rcount--;
            }
                
            displs[i] = displ;
            displ += sendcounts[i];
        }

        int mpi_rank;
        MPI_Comm_rank(comm, &mpi_rank);

        if (sendcounts[mpi_rank] > (int)recvsize) {
            errno = EINVAL;
            return -1;
        }

        if (MPI_Scatterv((void*)sendbuff, sendcounts, displs, MPI_BYTE, recvbuff, recvsize, MPI_BYTE, root_rank, comm) != MPI_SUCCESS) {
            errno = ECOMM;
            return -1;
        }

        recvsize = sendcounts[mpi_rank];

        delete [] sendcounts;
        delete [] displs;

        return recvsize;
    }

    void close(bool close_wr=true, bool close_rd=true) {
		closing = true;		
    }

    void finalize(bool, std::string name="") {
		if (!closing)
			this->close(true,true);
		
        MPI_Group_free(&group);
        MPI_Comm_free(&comm);
    }
};

class GatherMPI : public MPICollective {
    size_t* probe_data;
    size_t  EOS = 0;

public:
    GatherMPI(std::vector<Handle*> participants, bool root, int uniqtag) : MPICollective(participants, root, uniqtag) {
        probe_data = new size_t[participants.size()+1];
    }


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
        if (recvsize == 0) MTCL_MPI_PRINT(0, "[internal]:\t Gather::sendrecv \"recvsize\" is equal to zero!\n");

        if (recvsize % datasize != 0) {
            errno = EINVAL;
            return -1;
        }

        int nparticipants;
        MPI_Comm_size(comm, &nparticipants);

        size_t datacount = recvsize / datasize;

        int *recvcounts = new int[nparticipants]();
        int *displs = new int[nparticipants]();
        
        int displ = 0;

        int recvcount = (datacount / nparticipants) * datasize;
        int rcount = datacount % nparticipants;
            
        for (int i = 0; i < nparticipants; i++) {
            recvcounts[i] = recvcount;
                
            if (rcount > 0) {
                recvcounts[i] += datasize;
                rcount--;
            }
                
            displs[i] = displ;
            displ += recvcounts[i];
        }

        int mpi_rank;
        MPI_Comm_rank(comm, &mpi_rank);
        
        if (recvcounts[mpi_rank] > (int)sendsize) {
            errno = EINVAL;
            return -1;
        }

        if (MPI_Gatherv((void*)sendbuff, recvcounts[mpi_rank], MPI_BYTE, recvbuff, recvcounts, displs, MPI_BYTE, 0, comm) != MPI_SUCCESS) {
            errno = ECOMM;
            return -1;
        }

        sendsize = recvcounts[mpi_rank];

        delete [] recvcounts;
        delete [] displs;

        return sendsize;
    }

    void close(bool close_wr=true, bool close_rd=true) {
		closing = true;
    }

    void finalize(bool, std::string name="") {
		if(!closing) 
			this->close(true, true);
					
        MPI_Group_free(&group);
        MPI_Comm_free(&comm);
    }
};

#endif //MPICOLLIMPL_HPP
