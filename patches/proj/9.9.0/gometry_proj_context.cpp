#define FROM_PROJ_CPP

#include <memory>
#include <cmath>
#include <cstring>
#include <string>

#include "filemanager.hpp"
#include "proj_internal.h"
#include "proj/crs.hpp"

#if PROJ_VERSION_MAJOR != 9 || PROJ_VERSION_MINOR != 9 || PROJ_VERSION_PATCH != 0
#error "gometry_proj_context.cpp requires bundled PROJ 9.9.0"
#endif

extern "C" PJ_CONTEXT *gometry_proj_context_create() noexcept;
extern "C" void gometry_proj_clear_database_cache() noexcept;
extern "C" int gometry_proj_context_enable_default_angular_units(
    PJ_CONTEXT *context) noexcept;
extern "C" double
gometry_proj_conventional_period(double radians_per_unit) noexcept;

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

extern "C" void gometry_proj_clear_database_cache() noexcept {
    pj_clear_sqlite_cache();
}

extern "C" int gometry_proj_context_enable_default_angular_units(
    PJ_CONTEXT *context) noexcept {
    if (context == nullptr) {
        return 0;
    }
    context->gometryDefaultAngularUnits = true;
    return 1;
}

namespace {
struct GometryDoubleDouble {
    double high;
    double low;
};

GometryDoubleDouble gometry_two_sum(double left, double right) noexcept {
    const double sum = left + right;
    const double right_virtual = sum - left;
    const double error =
        (left - (sum - right_virtual)) + (right - right_virtual);
    return {sum, error};
}

GometryDoubleDouble gometry_add(GometryDoubleDouble left,
                                GometryDoubleDouble right) noexcept {
    const auto leading = gometry_two_sum(left.high, right.high);
    const auto trailing = gometry_two_sum(left.low, right.low);
    const auto middle = gometry_two_sum(leading.low, trailing.high);
    const auto normalized = gometry_two_sum(leading.high, middle.high);
    return gometry_two_sum(normalized.high,
                           normalized.low + middle.low + trailing.low);
}

GometryDoubleDouble gometry_negate(GometryDoubleDouble value) noexcept {
    return {-value.high, -value.low};
}

GometryDoubleDouble gometry_multiply(GometryDoubleDouble left,
                                     GometryDoubleDouble right) noexcept {
    const double product = left.high * right.high;
    const double error = std::fma(left.high, right.high, -product) +
                         left.high * right.low + left.low * right.high;
    const auto normalized = gometry_two_sum(product, error);
    return normalized;
}

GometryDoubleDouble gometry_divide(GometryDoubleDouble numerator,
                                   GometryDoubleDouble denominator) noexcept {
    const double first = numerator.high / denominator.high;
    const auto residual =
        gometry_add(numerator, gometry_negate(
                                   gometry_multiply(denominator, {first, 0.0})));
    const double second =
        (residual.high + residual.low) / denominator.high;
    return gometry_add({first, 0.0}, {second, 0.0});
}

double gometry_round(GometryDoubleDouble value) noexcept {
    return value.high + value.low;
}

GometryDoubleDouble gometry_selected_pm_degrees(
    double value, double radians_per_unit) noexcept {
    const double period =
        gometry_proj_conventional_period(radians_per_unit);
    if (period > 0.0) {
        return gometry_multiply(
            {value, 0.0},
            gometry_divide({360.0, 0.0}, {period, 0.0}));
    }
    return gometry_multiply(
        gometry_multiply({value, 0.0}, {radians_per_unit, 0.0}),
        gometry_divide({180.0, 0.0}, {M_PI, 0.0}));
}

struct GometryGeographicMetadata {
    double longitude_period = 0.0;
    int longitude_orientation = 0;
    double prime_meridian_value = 0.0;
    double prime_meridian_factor = 0.0;
    bool valid = false;
};

using GometryPj = std::unique_ptr<PJ, decltype(&proj_destroy)>;

bool gometry_read_geographic_metadata(PJ_CONTEXT *context, const PJ *object,
                                      GometryGeographicMetadata &metadata) {
    if (object == nullptr)
        return false;
    const PJ_TYPE type = proj_get_type(object);
    if (type == PJ_TYPE_COORDINATE_METADATA ||
        type == PJ_TYPE_BOUND_CRS) {
        GometryPj base(proj_get_source_crs(context, object), proj_destroy);
        return gometry_read_geographic_metadata(context, base.get(), metadata);
    }
    if (type == PJ_TYPE_COMPOUND_CRS) {
        for (int index = 0;; ++index) {
            GometryPj component(proj_crs_get_sub_crs(context, object, index),
                                proj_destroy);
            if (!component)
                return metadata.valid;
            GometryGeographicMetadata candidate;
            if (gometry_read_geographic_metadata(context, component.get(),
                                                 candidate)) {
                if (metadata.valid)
                    return false;
                metadata = candidate;
            }
        }
    }
    if (type != PJ_TYPE_GEOGRAPHIC_2D_CRS &&
        type != PJ_TYPE_GEOGRAPHIC_3D_CRS)
        return false;

    GometryPj coordinate_system(
        proj_crs_get_coordinate_system(context, object), proj_destroy);
    if (!coordinate_system)
        return false;
    const int count =
        proj_cs_get_axis_count(context, coordinate_system.get());
    bool has_longitude = false;
    bool has_latitude = false;
    for (int index = 0; index < count; ++index) {
        const char *direction = nullptr;
        double factor = 0.0;
        if (!proj_cs_get_axis_info(context, coordinate_system.get(), index,
                                   nullptr, nullptr, &direction, &factor,
                                   nullptr, nullptr, nullptr))
            return false;
        if (direction != nullptr &&
            (std::strcmp(direction, "east") == 0 ||
             std::strcmp(direction, "west") == 0)) {
            if (has_longitude)
                return false;
            const double period =
                gometry_proj_conventional_period(factor);
            metadata.longitude_period =
                period > 0.0 ? period : (2.0 * M_PI) / factor;
            metadata.longitude_orientation =
                std::strcmp(direction, "west") == 0 ? -1 : 1;
            has_longitude = true;
        } else if (direction != nullptr &&
                   (std::strcmp(direction, "north") == 0 ||
                    std::strcmp(direction, "south") == 0)) {
            if (has_latitude)
                return false;
            has_latitude = true;
        }
    }
    if (!has_longitude || !has_latitude ||
        !std::isfinite(metadata.longitude_period) ||
        metadata.longitude_period <= 0.0)
        return false;

    GometryPj prime_meridian(proj_get_prime_meridian(context, object),
                             proj_destroy);
    if (!prime_meridian)
        return false;
    if (!proj_prime_meridian_get_parameters(
            context, prime_meridian.get(), &metadata.prime_meridian_value,
            &metadata.prime_meridian_factor, nullptr))
        return false;
    metadata.valid = std::isfinite(metadata.prime_meridian_value) &&
                     std::isfinite(metadata.prime_meridian_factor) &&
                     metadata.prime_meridian_factor > 0.0;
    return metadata.valid;
}
} // namespace

extern "C" double gometry_adjust_longitude_rotation_degrees(
    double offset_degrees, double source_value, double source_factor,
    double target_value, double target_factor, int same_unit) noexcept {
    if (!std::isfinite(offset_degrees) || !std::isfinite(source_value) ||
        !std::isfinite(target_value) || !std::isfinite(source_factor) ||
        !std::isfinite(target_factor) || source_factor <= 0.0 ||
        target_factor <= 0.0)
        return offset_degrees;

    const auto selected_delta = gometry_add(
        gometry_selected_pm_degrees(source_value, source_factor),
        gometry_negate(
            gometry_selected_pm_degrees(target_value, target_factor)));
    const double declared_offset =
        same_unit != 0
            ? ((source_value - target_value) * source_factor) /
                  (M_PI / 180.0)
            : (source_value * source_factor) / (M_PI / 180.0) -
                  (target_value * target_factor) / (M_PI / 180.0);
    if (offset_degrees == declared_offset)
        return gometry_round(selected_delta);

    const auto residual = gometry_two_sum(offset_degrees, -declared_offset);
    return gometry_round(
        gometry_add(selected_delta, {residual.high, residual.low}));
}

extern "C" void gometry_proj_configure_default_noop_phase(
    PJ_CONTEXT *context, PJ *operation, const PJ *source_crs,
    const PJ *target_crs) noexcept {
    if (context == nullptr || operation == nullptr ||
        !context->gometryDefaultAngularUnits ||
        operation->alternativeCoordinateOperations.size() > 1)
        return;
    const PJ *selected = operation->alternativeCoordinateOperations.empty()
                             ? operation
                             : operation->alternativeCoordinateOperations.front().pj;
    if (selected == nullptr || selected->short_name == nullptr ||
        std::strcmp(selected->short_name, "noop") != 0)
        return;

    GometryGeographicMetadata source;
    GometryGeographicMetadata target;
    if (!gometry_read_geographic_metadata(context, source_crs, source) ||
        !gometry_read_geographic_metadata(context, target_crs, target) ||
        source.longitude_period != target.longitude_period ||
        source.longitude_orientation != target.longitude_orientation)
        return;

    const auto selected_source =
        gometry_selected_pm_degrees(source.prime_meridian_value,
                                    source.prime_meridian_factor);
    const auto selected_target =
        gometry_selected_pm_degrees(target.prime_meridian_value,
                                    target.prime_meridian_factor);
    auto phase_degrees =
        gometry_add(selected_source, gometry_negate(selected_target));
    auto phase_native = gometry_multiply(
        phase_degrees,
        gometry_divide({source.longitude_period, 0.0}, {360.0, 0.0}));
    if (source.longitude_orientation < 0)
        phase_native = gometry_negate(phase_native);
    if (phase_native.high == 0.0 && phase_native.low == 0.0)
        return;

    operation->gometryIdentityPhase = true;
    operation->gometryIdentityPhaseHigh = phase_native.high;
    operation->gometryIdentityPhaseLow = phase_native.low;
}
