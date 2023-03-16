#ifndef SHM_BUFFER_HPP
#define SHM_BUFFER_HPP

#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <cmath>
#include <atomic>
#include <mutex>
#include <condition_variable>

#include <pthread.h>

/*
 * shared-memory buffer, one single slot of SHM_SMAL_MSG_SIZE size
 */
class shmBuffer {
protected:
	struct buffer_element_t {
		size_t  size;
		char    data[SHM_SMALL_MSG_SIZE];
	};
	struct shmSegment {
		pthread_spinlock_t spinlock;
		void* guard;
		buffer_element_t data;
	} *shmp = nullptr;

	
	std::string segmentname{};
	std::atomic<bool> opened{false};

    std::mutex mutex;

	int createBuffer(const std::string& name, bool force=false) {
		int flags = O_CREAT|O_RDWR|O_EXCL;
		if (force) flags |= O_TRUNC;
		int fd = shm_open(name.c_str(), flags, S_IRUSR|S_IWUSR);
		if (fd == -1) return -1;
		if (ftruncate(fd, sizeof(shmSegment)) == -1) return -1;
		shmp = (shmSegment*)mmap(NULL, sizeof(shmSegment), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
		::close(fd);
		if (shmp == MAP_FAILED) {
			shmp = nullptr;
			return -1;
		}
		int rc;
		if ((rc=posix_madvise(shmp, sizeof(shmSegment), POSIX_MADV_SEQUENTIAL))==-1) {
			MTCL_SHM_PRINT(100, "shmBuffer::createBuffer, ERROR madvise errno=%d\n", rc);
		}
		if ((rc=pthread_spin_init(&shmp->spinlock, PTHREAD_PROCESS_SHARED)) != 0) {
			MTCL_SHM_PRINT(100, "shmBuffer::createBuffer, ERROR pthread_spin_init errno=%d\n", rc);
			return -1;
		}
		
		shmp->guard = nullptr;		
		segmentname=name;
		opened=true;
		return 0;
	}
public:

	shmBuffer() {}
	shmBuffer(const shmBuffer& o):shmp(o.shmp),segmentname(o.segmentname),opened(o.opened.load()) {}
	
	const std::string& name() {return segmentname;}
	
	// creates a shared-memory buffer with a name
	int create(const std::string name, bool force=false) {
		if constexpr (SHM_SMALL_MSG_SIZE<sizeof(size_t)) {
			errno = EINVAL;
			return -1;
		}
		return createBuffer(name,force);
	}
	const bool isOpen() { return opened;}
	// opens an existing shared-memory buffer
	int open(const std::string name) {
		if constexpr (SHM_SMALL_MSG_SIZE<sizeof(size_t)) {
			errno = EINVAL;
			return -1;
		}
		if (opened) {
			errno = EPERM;
			return -1;
		}
		int fd = shm_open(name.c_str(), O_RDWR, 0); 
		if (fd == -1)
			return -1;
		//struct stat sb;
		//if (fstat(fd, &sb) == -1) return -1;
		//assert(sb.st_size == sizeof(shmSegment));
		shmp = (shmSegment*)mmap(NULL, sizeof(shmSegment), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
		if (shmp == MAP_FAILED) return -1;
		::close(fd);
		int rc;
		if ((rc=posix_madvise(shmp, sizeof(shmSegment), POSIX_MADV_SEQUENTIAL))==-1) {
			MTCL_SHM_PRINT(100, "shmBuffer::open, ERROR madvise errno=%d\n", rc);
		}
		segmentname=name;
		opened = true;		
		return 0;
	}
	// closes and destroys (unlink=true) a shared-memory buffer previously
	// created or opened
	int close(bool unlink=false) {
		if (!shmp) {
			errno = EPERM;
			return -1;
		}
		munmap(shmp,sizeof(shmSegment));
		if (unlink) shm_unlink(segmentname.c_str());
		shmp=nullptr;
		opened = false;
		return 0;
	}	
	// adds a message to the buffer
	ssize_t put(const void* data, const size_t sz) {
		if (!shmp || !data) {
			errno=EINVAL;
			return -1;
		}

		std::unique_lock lk(mutex);
		if (sz==0) {
			do {
				pthread_spin_lock(&shmp->spinlock);
				if (shmp->guard==0) break;
				pthread_spin_unlock(&shmp->spinlock);
				cpu_relax();
			} while(1);
			shmp->data.size=sz;
			shmp->guard=(void*)data;
			pthread_spin_unlock(&shmp->spinlock);
			return 0;
		}
		posix_madvise((void*)data, sz, POSIX_MADV_SEQUENTIAL);
		for (size_t size = sz, s=0, p=0; size>0; size-=s, p+=s) {
			do {
				pthread_spin_lock(&shmp->spinlock);
				if (shmp->guard==0) break;
				pthread_spin_unlock(&shmp->spinlock);
				cpu_relax();
			} while(1);
			shmp->data.size=sz;
			s = std::min(size, (size_t)SHM_SMALL_MSG_SIZE);
			memcpy(shmp->data.data, (char*)data + p, s);
			shmp->guard = (void*)data;
			pthread_spin_unlock(&shmp->spinlock);
		}
		posix_madvise((void*)data, sz, POSIX_MADV_NORMAL);
		return sz;
	}
	// retrieves a message from the buffer, it blocks if the buffer is empty	
	ssize_t get(void* data, const size_t sz) {
		if (!shmp || !data || !sz) {
			errno=EINVAL;
			return -1;
		}
		
		std::unique_lock lk(mutex);

		do {
			pthread_spin_lock(&shmp->spinlock);
			if (shmp->guard != nullptr) {
				break;
			}
			pthread_spin_unlock(&shmp->spinlock);
			cpu_relax();
		} while(true);				

		size_t size = shmp->data.size;
		if (size==0) {
			shmp->guard=0;
			pthread_spin_unlock(&shmp->spinlock);
			return 0;
		}
		posix_madvise(data, sz, POSIX_MADV_SEQUENTIAL);
		int nmsgs = std::ceil((float)size / SHM_SMALL_MSG_SIZE);
		for (size_t sz=size, s=0, p=0; nmsgs; sz-=s, p+=s) {
			s = std::min(sz, (size_t)SHM_SMALL_MSG_SIZE);
			memcpy((char*)data + p, shmp->data.data, s);
			shmp->guard = 0;
			pthread_spin_unlock(&shmp->spinlock);
			if (--nmsgs == 0) break;
			do {
				pthread_spin_lock(&shmp->spinlock);
				if (shmp->guard!=0) break;
				pthread_spin_unlock(&shmp->spinlock);
				cpu_relax();
			} while(true);
		}
		posix_madvise(data, sz, POSIX_MADV_NORMAL);
		return size;
	}
	// retrieves the size of the message in the buffer without removing the message
	// from the buffer, it blocks if the buffer is empty	
	ssize_t getsize() {
		if (!shmp) {
			errno=EINVAL;
			return -1;
		}
		//std::unique_lock lk(mutex);
		do {
			pthread_spin_lock(&shmp->spinlock);
			if (shmp->guard != nullptr) {
				break;
			}
			pthread_spin_unlock(&shmp->spinlock);
			cpu_relax();
		} while(true);				
		size_t size = shmp->data.size;
		pthread_spin_unlock(&shmp->spinlock);
		return size;
	}
	// retrieves a message from the buffer, it doesn't block if the buffer is empty	
	ssize_t tryget(void* data, const size_t sz) {
		if (!shmp || !data || !sz) {
			errno=EINVAL;
			return -1;
		}

		std::unique_lock lk(mutex);

		pthread_spin_lock(&shmp->spinlock);
		if (shmp->guard == nullptr) {
			pthread_spin_unlock(&shmp->spinlock);
			errno = EAGAIN;
			return -1;
		}
		size_t size = shmp->data.size;
		if (size==0) {
			shmp->guard=0;
			pthread_spin_unlock(&shmp->spinlock);
			return 0;
		}
		posix_madvise(data, sz, POSIX_MADV_SEQUENTIAL);
		int nmsgs = std::ceil((float)size / SHM_SMALL_MSG_SIZE);
		for (size_t sz=size, s=0, p=0; nmsgs; sz-=s, p+=s) {
			s = std::min(sz, (size_t)SHM_SMALL_MSG_SIZE);
			memcpy((char*)data + p, shmp->data.data, s);
			shmp->guard = 0;
			pthread_spin_unlock(&shmp->spinlock);
			if (--nmsgs == 0) break;
			do {
				pthread_spin_lock(&shmp->spinlock);
				if (shmp->guard!=0) break;
				pthread_spin_unlock(&shmp->spinlock);
				cpu_relax();
			} while(true);
		}
		posix_madvise(data, sz, POSIX_MADV_NORMAL);
		return size;
	}
	// retrieves the size of the message in the buffer without removing the message
	// from the buffer, it doesn't block if the buffer is empty	
	ssize_t trygetsize() {
		if (!shmp) {
			errno=EINVAL;
			return -1;
		}
		//std::unique_lock lk(mutex);
		pthread_spin_lock(&shmp->spinlock);
		if (shmp->guard == nullptr) {
			pthread_spin_unlock(&shmp->spinlock);
			errno = EAGAIN;
			return -1;
		}
		size_t size = shmp->data.size;
		pthread_spin_unlock(&shmp->spinlock);
		return size;
	}
	// it peeks at whether there are any messages in the buffer
	// WARNING: The buffer may already be emptied by the time 'pick' returns.
	ssize_t peek() {
		pthread_spin_lock(&shmp->spinlock);
		int val = ((shmp->guard)?1:0);
		pthread_spin_unlock(&shmp->spinlock);
		return val;
	}
};

#endif
