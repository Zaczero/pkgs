/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  Implementation of the aea (Albers Equal Area) projection.
 *           and the leac (Lambert Equal Area Conic) projection
 * Author:   Gerald Evenden (1995)
 *           Thomas Knudsen (2016) - revise/add regression tests
 *
 ******************************************************************************
 * Copyright (c) 1995, Gerald Evenden
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *****************************************************************************/

#include "proj.h"
#include "proj_internal.h"
#include <errno.h>
#include <math.h>
#include <limits>

#define EPS10 1.e-10

PROJ_HEAD(aea, "Albers Equal Area") "\n\tConic Sph&Ell\n\tlat_1= lat_2=";
PROJ_HEAD(leac, "Lambert Equal Area Conic")
"\n\tConic, Sph&Ell\n\tlat_1= south";

namespace { // anonymous namespace
/* Only endpoint roundoff may be clamped; interior ratios always invert. */
constexpr double DOMAIN_ROUNDING =
    16.0 * std::numeric_limits<double>::epsilon();
/* Karney's sixth-order auxiliary-latitude series is full-binary64 accurate
 * for ordinary flattening |f| <= 1/150. */
constexpr double AUTHALIC_SERIES_MAX_FLATTENING = 1.0 / 150.0;
constexpr int AUTHALIC_ORDER = int(AuxLat::ORDER);

/* Invert q(sin(phi)) with a monotone bracket when the ordinary series is not
 * within Karney's documented full-binary64 flattening range. */
static bool authalic_inverse_strong(double ratio, double qp, const PJ *P,
                                    double &phi) {
    const double target = qp * ratio;
    double lo = -1.0;
    double hi = 1.0;
    double current = ratio;
    double best = current;
    double best_residual = HUGE_VAL;

    for (;;) {
        const double sinphi = current;
        const double residual = pj_authalic_lat_q(sinphi, P) - target;
        if (!isfinite(residual))
            return false;
        const double absolute_residual = fabs(residual);
        if (absolute_residual < best_residual) {
            best = sinphi;
            best_residual = absolute_residual;
        }
        if (residual == 0.0) {
            phi = asin(current);
            return isfinite(phi);
        }
        if (residual < 0.0)
            lo = current;
        else
            hi = current;

        const double midpoint = lo + 0.5 * (hi - lo);
        if (!(midpoint > lo && midpoint < hi)) {
            const double lo_residual = pj_authalic_lat_q(lo, P) - target;
            const double hi_residual = pj_authalic_lat_q(hi, P) - target;
            if (!isfinite(lo_residual) || !isfinite(hi_residual))
                return false;
            if (fabs(lo_residual) < best_residual) {
                best = lo;
                best_residual = fabs(lo_residual);
            }
            if (fabs(hi_residual) < best_residual)
                best = hi;
            phi = asin(best);
            return isfinite(phi);
        }

        const double denominator = 1.0 - P->es * sinphi * sinphi;
        if (!isfinite(denominator) || denominator <= 0.0)
            return false;
        const double derivative =
            2.0 * P->one_es / (denominator * denominator);
        if (!isfinite(derivative) || derivative <= 0.0)
            return false;
        const double quarter_width = 0.25 * (hi - lo);
        double trial = current - residual / derivative;
        if (!(trial > lo + quarter_width
              && trial < hi - quarter_width)
            || !isfinite(trial))
            trial = midpoint;
        if (trial == current)
            trial = midpoint;
        current = trial;
    }
}

struct pj_aea {
    double n;
    double c;
    double dd;
    double n2;
    double rho0;
    double qp;
    double apa[AUTHALIC_ORDER];
    double phi1;
    double phi2;
    int ellips;
    int authalic_series;
};

} // anonymous namespace

static PJ *pj_aea_destructor(PJ *P, int errlev) { /* Destructor */
    if (nullptr == P)
        return nullptr;

    if (nullptr == P->opaque)
        return pj_default_destructor(P, errlev);

    return pj_default_destructor(P, errlev);
}

static PJ_XY aea_e_forward(PJ_LP lp, PJ *P) { /* Ellipsoid/spheroid, forward */
    PJ_XY xy = {0.0, 0.0};
    struct pj_aea *Q = static_cast<struct pj_aea *>(P->opaque);
    const double radial =
        Q->ellips ? Q->c - Q->n * pj_authalic_lat_q(sin(lp.phi), P)
                  : Q->c - Q->n2 * sin(lp.phi);
    if (radial < 0.) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return xy;
    }
    const double rho = Q->dd * sqrt(radial);
    lp.lam *= Q->n;
    xy.x = rho * sin(lp.lam);
    xy.y = Q->rho0 - rho * cos(lp.lam);
    return xy;
}

static PJ_LP aea_e_inverse(PJ_XY xy, PJ *P) { /* Ellipsoid/spheroid, inverse */
    PJ_LP lp = {0.0, 0.0};
    struct pj_aea *Q = static_cast<struct pj_aea *>(P->opaque);
    xy.y = Q->rho0 - xy.y;
    double rho = hypot(xy.x, xy.y);
    const bool zero_radius = rho == 0.0;
    if (Q->n < 0.) {
        rho = -rho;
        xy.x = -xy.x;
        xy.y = -xy.y;
    }

    double ratio;
    if (Q->ellips) {
        const double radial = rho / Q->dd;
        const double q = (Q->c - radial * radial) / Q->n;
        ratio = q / Q->qp;
    } else {
        const double radial = rho / Q->dd;
        ratio = (Q->c - radial * radial) / Q->n2;
    }
    if (!isfinite(ratio) || fabs(ratio) > 1.0 + DOMAIN_ROUNDING) {
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
        return lp;
    }
    if (ratio > 1.0)
        ratio = 1.0;
    else if (ratio < -1.0)
        ratio = -1.0;

    if (Q->ellips) {
        if (ratio == 1.0)
            lp.phi = M_HALFPI;
        else if (ratio == -1.0)
            lp.phi = -M_HALFPI;
        else {
            const double beta = asin(ratio);
            if (Q->authalic_series)
                lp.phi = pj_auxlat_convert(beta, Q->apa);
            else if (!authalic_inverse_strong(ratio, Q->qp, P, lp.phi)) {
                proj_errno_set(
                    P, PROJ_ERR_COORD_TRANSFM_OUTSIDE_PROJECTION_DOMAIN);
                return lp;
            }
        }
    } else {
        lp.phi = asin(ratio);
    }
    lp.lam = zero_radius ? 0.0 : atan2(xy.x, xy.y) / Q->n;
    return lp;
}

static PJ *setup(PJ *P) {
    struct pj_aea *Q = static_cast<struct pj_aea *>(P->opaque);

    P->inv = aea_e_inverse;
    P->fwd = aea_e_forward;

    if (fabs(Q->phi1) > M_HALFPI) {
        proj_log_error(P,
                       _("Invalid value for lat_1: |lat_1| should be <= 90°"));
        return pj_aea_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    if (fabs(Q->phi2) > M_HALFPI) {
        proj_log_error(P,
                       _("Invalid value for lat_2: |lat_2| should be <= 90°"));
        return pj_aea_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    if (fabs(Q->phi1 + Q->phi2) < EPS10) {
        proj_log_error(P, _("Invalid value for lat_1 and lat_2: |lat_1 + "
                            "lat_2| should be > 0"));
        return pj_aea_destructor(P, PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
    }
    double sinphi = sin(Q->phi1);
    Q->n = sinphi;
    double cosphi = cos(Q->phi1);
    const int secant = fabs(Q->phi1 - Q->phi2) >= EPS10;
    Q->ellips = (P->es > 0.);
    if (Q->ellips) {
        double ml1, m1;

        m1 = pj_msfn(sinphi, cosphi, P->es);
        ml1 = pj_authalic_lat_q(sinphi, P);
        if (secant) { /* secant cone */
            double ml2, m2;

            sinphi = sin(Q->phi2);
            cosphi = cos(Q->phi2);
            m2 = pj_msfn(sinphi, cosphi, P->es);
            ml2 = pj_authalic_lat_q(sinphi, P);
            if (ml2 == ml1)
                return pj_aea_destructor(P, 0);

            Q->n = (m1 * m1 - m2 * m2) / (ml2 - ml1);
            if (Q->n == 0) {
                // Not quite, but es is very close to 1...
                proj_log_error(P, _("Invalid value for eccentricity"));
                return pj_aea_destructor(P,
                                         PROJ_ERR_INVALID_OP_ILLEGAL_ARG_VALUE);
            }
        }
        Q->qp = pj_authalic_lat_q(1.0, P);
        Q->authalic_series =
            fabs(P->f) <= AUTHALIC_SERIES_MAX_FLATTENING;
        if (Q->authalic_series)
            pj_auxlat_coeffs(P->n, AuxLat::AUTHALIC, AuxLat::GEOGRAPHIC, Q->apa);
        Q->c = m1 * m1 + Q->n * ml1;
        Q->dd = 1. / Q->n;
        Q->rho0 = Q->dd * sqrt(Q->c - Q->n * pj_authalic_lat_q(sin(P->phi0), P));
    } else {
        if (secant)
            Q->n = .5 * (Q->n + sin(Q->phi2));
        Q->n2 = Q->n + Q->n;
        Q->c = cosphi * cosphi + Q->n2 * sinphi;
        Q->dd = 1. / Q->n;
        Q->rho0 = Q->dd * sqrt(Q->c - Q->n2 * sin(P->phi0));
    }

    return P;
}

PJ *PJ_PROJECTION(aea) {
    struct pj_aea *Q =
        static_cast<struct pj_aea *>(calloc(1, sizeof(struct pj_aea)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = pj_aea_destructor;

    Q->phi1 = pj_param(P->ctx, P->params, "rlat_1").f;
    Q->phi2 = pj_param(P->ctx, P->params, "rlat_2").f;
    return setup(P);
}

PJ *PJ_PROJECTION(leac) {
    struct pj_aea *Q =
        static_cast<struct pj_aea *>(calloc(1, sizeof(struct pj_aea)));
    if (nullptr == Q)
        return pj_default_destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);
    P->opaque = Q;
    P->destructor = pj_aea_destructor;

    Q->phi2 = pj_param(P->ctx, P->params, "rlat_1").f;
    Q->phi1 = pj_param(P->ctx, P->params, "bsouth").i ? -M_HALFPI : M_HALFPI;
    return setup(P);
}

#undef EPS10
