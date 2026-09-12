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
constexpr int AUTHALIC_ORDER = 6;

/* Stable q(sin(phi)) and q_p evaluators shared by setup and inverse. */
static double authalic_q(double sinphi, const PJ *P) {
    if (P->e < 1.0e-7)
        return 2.0 * sinphi;
    const double esin = P->e * sinphi;
    const double denominator = 1.0 - esin * esin;
    return P->one_es *
           (sinphi / denominator + atanh(esin) / P->e);
}

static double authalic_poly(double n, const double *coeffs, int count) {
    double value = 0.0;
    for (int i = count - 1; i >= 0; --i)
        value = value * n + coeffs[i];
    return value;
}

/* Karney's sixth-order authalic-to-geographic coefficients. */
static void authalic_coeffs(double n, double apa[AUTHALIC_ORDER]) {
    static const double c0[] = {
        4.0 / 3.0, 4.0 / 45.0, -16.0 / 35.0, -2582.0 / 14175.0,
        60136.0 / 467775.0, 28112932.0 / 212837625.0};
    static const double c1[] = {
        46.0 / 45.0, 152.0 / 945.0, -11966.0 / 14175.0,
        -21016.0 / 51975.0, 251310128.0 / 638512875.0};
    static const double c2[] = {
        3044.0 / 2835.0, 3802.0 / 14175.0, -94388.0 / 66825.0,
        -8797648.0 / 10945935.0};
    static const double c3[] = {
        6059.0 / 4725.0, 41072.0 / 93555.0,
        -1472637812.0 / 638512875.0};
    static const double c4[] = {768272.0 / 467775.0,
                                455935736.0 / 638512875.0};
    apa[0] = n * authalic_poly(n, c0, 6);
    apa[1] = n * n * authalic_poly(n, c1, 5);
    apa[2] = n * n * n * authalic_poly(n, c2, 4);
    apa[3] = n * n * n * n * authalic_poly(n, c3, 3);
    apa[4] = n * n * n * n * n * authalic_poly(n, c4, 2);
    apa[5] = n * n * n * n * n * n * 4210684958.0 / 1915538625.0;
}

static double authalic_inverse_series(double beta,
                                      const double apa[AUTHALIC_ORDER]) {
    double phi = beta;
    for (int i = 0; i < AUTHALIC_ORDER; ++i)
        phi += apa[i] * sin(2.0 * (i + 1) * beta);
    return phi;
}

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
        const double residual = authalic_q(sinphi, P) - target;
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
            const double lo_residual = authalic_q(lo, P) - target;
            const double hi_residual = authalic_q(hi, P) - target;
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
        Q->ellips ? Q->c - Q->n * authalic_q(sin(lp.phi), P)
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
                lp.phi = authalic_inverse_series(beta, Q->apa);
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
        ml1 = authalic_q(sinphi, P);
        if (secant) { /* secant cone */
            double ml2, m2;

            sinphi = sin(Q->phi2);
            cosphi = cos(Q->phi2);
            m2 = pj_msfn(sinphi, cosphi, P->es);
            ml2 = authalic_q(sinphi, P);
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
        Q->qp = authalic_q(1.0, P);
        Q->authalic_series =
            fabs(P->f) <= AUTHALIC_SERIES_MAX_FLATTENING;
        if (Q->authalic_series)
            authalic_coeffs(P->n, Q->apa);
        Q->c = m1 * m1 + Q->n * ml1;
        Q->dd = 1. / Q->n;
        Q->rho0 = Q->dd * sqrt(Q->c - Q->n * authalic_q(sin(P->phi0), P));
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
