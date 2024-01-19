#ifndef COMMLIB_HPP
#define COMMLIB_HPP

namespace MTCL {


#if defined(ENABLE_MPI)
const bool MPI_ENABLED     = true;
#define MTCL_ENABLE_MPI
#else
const bool MPI_ENABLED     = false;
#endif
#if defined(ENABLE_MPIP2P) && !defined(NO_MTCL_MULTITHREADED)
const bool MPIP2P_ENABLED  = true;
#define MTCL_ENABLE_MPIP2P
#else
const bool MPIP2P_ENABLED  = false;
#endif
#if defined(ENABLE_UCX)
const bool UCX_ENABLED     = true;
const bool UCC_ENABLED     = true;
#define MTCL_ENABLE_UCX
#else
const bool UCX_ENABLED     = false;
const bool UCC_ENABLED     = false;
#endif

#ifdef ENABLE_SHM
#define MTCL_ENABLE_SHM
#endif

#ifdef ENABLE_MQTT
#define MTCL_ENABLE_MQTT
#endif

} // namespace


#include "config.hpp"
#include "utils.hpp"
#include "manager.hpp"

#endif
