#ifndef MPICOLLIMPL_HPP
#define MPICOLLIMPL_HPP

#include "collectiveImpl.hpp"
#include <mpi.h>
#include <cassert>

/**
 * @brief MPI implementation of collective operations. Abstract class, only provides
 * generic functionalities for collectives using the MPI transport. Subclasses must
 * implement collective-specific behavior.
 * 
 */
class MPICollective : public CollectiveImpl {
protected:
    bool root;               // root flag
    int rank_of_the_root;    // MPI rank of the root
	int my_group_rank;       // team rank
	int my_mpi_rank;         // MPI rank 
	int nparticipants;       // size of the team/group
    MPI_Comm comm;           
    MPI_Group group;
    
    MPI_Request request_header = MPI_REQUEST_NULL;
    bool closing = false;
    ssize_t last_probe = -1;

public:
    MPICollective(std::vector<Handle*> participants, size_t nparticipants, bool root, int rank, int uniqtag)
		: CollectiveImpl(participants, nparticipants, rank, uniqtag), root(root) {
		//
        //TODO: endianess conversion MUST BE added for all communications! 
		//
		int* ranks;
        MPI_Comm_rank(MPI_COMM_WORLD, &my_mpi_rank);
        int coll_size;
        if(root) {
            coll_size = participants.size() + 1;
            rank_of_the_root = my_mpi_rank;
            ranks = new int[participants.size()+1];
            ranks[0] = rank_of_the_root;    // NOTE: the collective root has always MPI rank 0

			int remote_rank;
			// Here we need to know which are the MPI ranks of peers participating to the team
			// IMPORTANT: the order is the one defined by the participant list!
            for(size_t i = 0; i < participants.size(); i++) {
                receiveFromHandle(participants.at(i), &remote_rank, sizeof(int));
                ranks[i+1] = remote_rank;
            }

			// Sending all ranks to all team member (but the root)
            for(auto& p : participants) {
                p->send(ranks, sizeof(int)*(participants.size()+1));  // TODO: what if it fails?
            }
        }
        else {
            participants.at(0)->send(&my_mpi_rank, sizeof(int));
            size_t sz;
            probeHandle(participants.at(0), sz, true);
            coll_size = sz/sizeof(int);
            ranks = new int[sz];
            receiveFromHandle(participants.at(0), ranks, sz);
			rank_of_the_root = ranks[0];
        }

        MPI_Group group_world;
        if (MPI_Comm_group(MPI_COMM_WORLD, &group_world) != MPI_SUCCESS) {
			MTCL_ERROR("[internal]:\t", "MPICollective::MPI_Comm_group\n");
		}
		// NOTE: the indexes of the ranks vector will be the new MPI rank of the processes!
        if (MPI_Group_incl(group_world, coll_size, ranks, &group) != MPI_SUCCESS) {
			MTCL_ERROR("[internal]:\t", "MPICollective::MPI_Group_incl\n");
		}
        if (MPI_Comm_create_group(MPI_COMM_WORLD, group, uniqtag, &comm) != MPI_SUCCESS) {
			MTCL_ERROR("[internal]:\t", "MPICollective::MPI_Comm_create_group\n");
		}

        delete[] ranks;

        MPI_Group_rank(group, &my_group_rank);

		assert(my_group_rank == rank);  // <--- CHECK!
		
		this->nparticipants = coll_size;
		if (this->nparticipants != (int)(CollectiveImpl::nparticipants)) {
			MTCL_ERROR("[internal]:\t", "MPICollective, nparticipants missmatch!!\n");
		}
		
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
    BroadcastMPI(std::vector<Handle*> participants, size_t nparticipants, bool root, int rank, int uniqtag) : MPICollective(participants, nparticipants, root, rank, uniqtag) {}


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
            if(MPI_Bcast((void*)sendbuff, sendsize, MPI_BYTE, 0, comm) != MPI_SUCCESS) {
                errno = ECOMM;
                return -1;
            }
            if (recvbuff) 
				memcpy(recvbuff, sendbuff, sendsize);
			
            return sendsize;
        }
        else {
            if(MPI_Bcast((void*)recvbuff, recvsize, MPI_BYTE, 0, comm) != MPI_SUCCESS) {
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
    ScatterMPI(std::vector<Handle*> participants, size_t nparticipants, bool root, int rank, int uniqtag) : MPICollective(participants, nparticipants, root, rank, uniqtag) {}

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
		MTCL_MPI_PRINT(100, "group rank=%d (MPI rank=%d), sendsize=%ld, recvsize=%ld, datasize=%ld\n",
					   my_group_rank, my_mpi_rank, sendsize, recvsize, datasize);

        if (sendsize == 0)
			MTCL_MPI_PRINT(0, "[internal]:\t Scatter::sendrecv \"sendsize\" is equal to zero!\n");

        if (sendsize % datasize != 0) {
            errno = EINVAL;
            return -1;
        }
		
        int datacount = sendsize / datasize;

        int *sendcounts = new int[nparticipants]();
        int *displs     = new int[nparticipants]();
        
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

  	    if (static_cast<size_t>(sendcounts[my_group_rank]) > recvsize) {
			MTCL_ERROR("[internal]:\t","receive buffer too small %ld instead of %ld (team rank=%d, MPI rank=%d)\n", recvsize, sendcounts[my_group_rank], my_group_rank, my_mpi_rank);
			
			fprintf(stderr, "nparticipants=%d\n", nparticipants);
			for (int i = 0; i < nparticipants; i++) 
				fprintf(stderr, "%d, %d\n", sendcounts[i], displs[i]);
			
            errno = EINVAL;
            return -1;
        }

        if (MPI_Scatterv((void*)sendbuff, sendcounts, displs, MPI_BYTE, recvbuff, recvsize, MPI_BYTE, 0, comm) != MPI_SUCCESS) {
            errno = ECOMM;
            return -1;
        }

        recvsize = sendcounts[my_group_rank];

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
public:
    GatherMPI(std::vector<Handle*> participants, size_t nparticipants, bool root, int rank, int uniqtag) : MPICollective(participants, nparticipants, root, rank, uniqtag) {
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
        if (recvsize == 0)
			MTCL_ERROR("[internal]:\t", "Gather::sendrecv \"recvsize\" is equal to zero, this is an ERROR!\n");

        if (recvsize % datasize != 0) {
            errno = EINVAL;
            return -1;
        }

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

        if ((size_t)recvcounts[my_group_rank] > sendsize) {
            MTCL_ERROR("[internal]:\t","sending buffer too small %ld instead of %ld\n", sendsize, recvcounts[my_group_rank]);
            errno = EINVAL;
            return -1;
        }

        if (MPI_Gatherv((void*)sendbuff, recvcounts[my_group_rank], MPI_BYTE, recvbuff, recvcounts, displs, MPI_BYTE, 0, comm) != MPI_SUCCESS) {
            errno = ECOMM;
            return -1;
        }
		
        sendsize = recvcounts[my_group_rank];

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

class AllGatherMPI : public MPICollective {
    public:
    
    AllGatherMPI(std::vector<Handle*> participants, size_t nparticipants, bool root, int rank, int uniqtag) : MPICollective(participants, nparticipants, root, rank, uniqtag) {}

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
        if (recvsize == 0)
			MTCL_ERROR("[internal]:\t", "AllGather::sendrecv \"recvsize\" is equal to zero, this is an ERROR!\n");

        if (recvsize % datasize != 0) {
            errno = EINVAL;
            return -1;
        }

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

        if ((size_t)recvcounts[my_group_rank] > sendsize) {
            MTCL_ERROR("[internal]:\t","sending buffer too small %ld instead of %ld\n", sendsize, recvcounts[my_group_rank]);
            errno = EINVAL;
            return -1;
        }

        if (MPI_Allgatherv((void*)sendbuff, recvcounts[my_group_rank], MPI_BYTE, recvbuff, recvcounts, displs, MPI_BYTE, comm) != MPI_SUCCESS) {
            errno = ECOMM;
            return -1;
        }
		
        sendsize = recvcounts[my_group_rank];

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
