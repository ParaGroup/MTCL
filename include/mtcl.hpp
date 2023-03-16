#ifndef COMMLIB_HPP
#define COMMLIB_HPP


#if defined(ENABLE_MPI)
const bool MPI_ENABLED     = true;
#else
const bool MPI_ENABLED     = false;
#endif
#if defined(ENABLE_MPIP2P)
const bool MPIP2P_ENABLED  = true;
#else
const bool MPIP2P_ENABLED  = false;
#endif
#if defined(ENABLE_UCX)
const bool UCX_ENABLED     = true;
const bool UCC_ENABLED     = true;
#else
const bool UCX_ENABLED     = false;
const bool UCC_ENABLED     = false;
#endif


#include "config.hpp"
#include "utils.hpp"
#include "manager.hpp"

#endif
