#define FROM_PROJ_CPP

#include <memory>

#include "filemanager.hpp"
#include "proj_internal.h"

#if PROJ_VERSION_MAJOR != 9 || PROJ_VERSION_MINOR != 6 || PROJ_VERSION_PATCH != 2
#error "gometry_proj_context.cpp requires bundled PROJ 9.6.2"
#endif

extern "C" PJ_CONTEXT *gometry_proj_context_create() noexcept;

extern "C" PJ_CONTEXT *gometry_proj_context_create() noexcept {
    try {
        auto context = std::make_unique<pj_ctx>();
        context->iniFileLoaded = true;
        context->endpoint = "https://cdn.proj.org";
        NS_PROJ::FileManager::fillDefaultNetworkInterface(context.get());
        return context.release();
    } catch (...) {
        return nullptr;
    }
}
