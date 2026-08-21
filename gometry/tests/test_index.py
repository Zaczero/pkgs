"""Spatial index and joins — candidates vs exact queries, explain plans,
nearest (planar and geodesic), mutability, and prepared geometry.
"""

import copy
from typing import Any, cast

import gometry as gm
import numpy as np
import pytest

from tests._support import floats, ids, pair_rows


def match_rows(matches: gm.Groups) -> list[list[int]]:
    return [row.tolist() for row in matches]


def test_index_and_prepared_geometry_reveal_held_geometry_frame() -> None:
    geometry = gm.box(0, 0, 1, 1, crs=4326, epoch=2020.0)
    index = gm.SpatialIndex([geometry])
    assert index.crs == geometry.crs
    assert index.epoch == geometry.epoch
    assert gm.SpatialIndex().crs is None
    assert gm.SpatialIndex().epoch is None

    prepared = geometry.prepare()
    assert isinstance(prepared.geometry, gm.Polygon)
    assert prepared.geometry is not geometry
    assert prepared.geometry == geometry


def test_mutable_index_clones_own_geodesic_caps_cache() -> None:
    """Equal row counts/generations do not identify independently mutable clones."""
    left = gm.SpatialIndex()
    right = copy.copy(left)

    # Frozen 100-box repro (seed 42); full size required for shared-cache bug.
    _LEFT_BOXES = [
        (47.405111475680485, -66.49849426882663, 49.67784309079653, -64.63512943745093),
        (80.4002128157642, 24.737928239207577, 87.54843140063248, 25.524745016979963),
        (
            -26.54658130700807,
            -65.82838927867016,
            -24.7193413060596,
            -61.736082502653595,
        ),
        (
            -160.97777030748637,
            -42.16272890386921,
            -155.74368324902812,
            -37.7576912071038,
        ),
        (-95.05018850616312, 12.497195742627227, -88.5556878984083, 12.648535944083909),
        (
            103.97854562315467,
            27.739515298351762,
            106.76652470364681,
            29.067803346864835,
        ),
        (
            155.4524445503056,
            -22.87676368423225,
            156.28513671300877,
            -22.012704307247887,
        ),
        (118.1480845581363, 14.521644391364745, 124.6243979170039, 20.386525506245906),
        (12.317551094598258, 66.23620695711188, 15.40797267454425, 70.69732794417037),
        (
            111.99758584601824,
            16.592765330994453,
            118.90507035847338,
            21.253847278522873,
        ),
        (69.55442431307398, -63.58458628820729, 71.4548206907212, -61.19842137575064),
        (
            -142.87072784596666,
            -37.40927590945577,
            -141.9728165536298,
            -35.11328444488604,
        ),
        (
            46.13271104989607,
            -18.923494944188207,
            49.15714069011945,
            -17.168389401093453,
        ),
        (-79.22754050330146, 61.13164227974917, -74.00806095985337, 66.04377722451838),
        (-111.81285961264703, 32.07775171304888, -110.42197991192779, 35.1754497029343),
        (
            166.43793921644237,
            19.599966379573004,
            170.93784219226208,
            25.108418962393014,
        ),
        (116.56965286453527, 38.63998761647427, 118.47913263305169, 38.99357954331617),
        (
            -62.74596365991215,
            -32.51627736340162,
            -60.97919919558017,
            -24.967290620154692,
        ),
        (
            127.96499300070741,
            -25.94509668821309,
            133.24295845653697,
            -22.719604669833842,
        ),
        (140.9461805117848, -5.760740637764172, 143.1387338271194, -3.712383326981703),
        (
            20.865165615471255,
            -33.21617480678906,
            25.583394938237227,
            -26.023374026329495,
        ),
        (
            -34.20382825226494,
            -39.295093717980336,
            -26.22328116095357,
            -35.16983599793627,
        ),
        (
            -139.09079986091007,
            -63.40370744053716,
            -138.12457173113987,
            -58.346883711082754,
        ),
        (99.30698388340778, -10.897604648044236, 99.90885276200824, -7.782812284642596),
        (168.68126928163292, 4.076008313879186, 176.4527884647805, 10.97616796153172),
        (
            -166.09645253944132,
            30.901054710427246,
            -160.6109406241314,
            35.24312032065673,
        ),
        (-79.27943541613546, 19.73465180117313, -78.29817324472803, 23.26929728145906),
        (-15.73393984806981, 63.53422985295123, -8.714701619082076, 65.71500335388484),
        (0.1992784371014409, -44.98873672578161, 7.509038367925523, -38.01164002407115),
        (
            -68.52877090746482,
            19.452929281240728,
            -63.617906237103256,
            20.760359502782844,
        ),
        (89.25367202555145, 5.513064216747608, 95.50482120673286, 9.80285822708951),
        (
            -169.8055553164992,
            -24.618152019345764,
            -169.55168905165112,
            -17.178272950855288,
        ),
        (128.76543845988266, 46.43317411056512, 131.29480005056368, 46.9907829258692),
        (128.5232637293738, 62.57292234171916, 129.29992600071006, 66.51224700192041),
        (
            -146.46774372074958,
            36.484303136012414,
            -140.31765172922437,
            37.59859570556054,
        ),
        (-8.403991446431348, 6.972503089292147, -6.210044077804881, 13.96472411386568),
        (
            -26.133100331698444,
            -40.34825123810852,
            -21.772661230340724,
            -34.4817957922977,
        ),
        (
            -101.6086384475034,
            -26.359719217874705,
            -93.64695852988233,
            -21.12568256252302,
        ),
        (
            -21.045971469068633,
            2.4606177449826845,
            -19.990038321709335,
            4.335726707531988,
        ),
        (
            -55.05090886986518,
            12.363220584012666,
            -53.133002482352225,
            14.202937921179986,
        ),
        (
            -145.86235075692895,
            18.354414017813852,
            -143.95371066482082,
            25.607232120562262,
        ),
        (122.27603608627379, -60.07997101558852, 124.25627269778886, -54.6950465670479),
        (-97.15948549405087, -51.4763411784965, -89.66892299346357, -46.86510074122675),
        (
            -9.291851053990001,
            39.84671940070547,
            -2.8126247716335184,
            41.450957724164304,
        ),
        (
            -137.04352316220007,
            -9.652834463107148,
            -133.5972520403427,
            -5.863339585617416,
        ),
        (77.88578881634919, 24.27103662106221, 85.76069398614032, 25.148537803162682),
        (
            -33.108764085228614,
            -22.497635244705158,
            -26.201551048041566,
            -20.4332502067349,
        ),
        (
            -105.32897113012409,
            -7.194103303361537,
            -101.8961061754323,
            -4.893596660492706,
        ),
        (-85.06580772008583, 59.257183898641784, -81.46507483416352, 66.16184182626024),
        (
            17.110606232948328,
            -62.917633866516624,
            25.104937733408867,
            -56.213015944385006,
        ),
    ]
    _RIGHT_BOXES = [
        (159.4587274768154, 59.69137762113786, 166.2634237786884, 61.10523539490878),
        (
            -4.882017346755731,
            -40.075378112114564,
            -1.613799035615055,
            -39.51215845233435,
        ),
        (-41.149139547848534, 67.94323812916164, -38.95403538828853, 74.23739588455534),
        (
            -15.297155104691285,
            -10.778951961377189,
            -7.634345741899867,
            -2.8151127143847496,
        ),
        (
            18.961229957910206,
            30.577158541485645,
            20.284124877575312,
            33.021150362892705,
        ),
        (159.36118408951398, 11.08524071427587, 163.74452618037074, 17.094247641270478),
        (
            -150.56380721145575,
            11.784863224255972,
            -146.49128918639158,
            18.621350371437426,
        ),
        (
            -116.47287250057569,
            64.50904645842306,
            -115.73999192517505,
            66.07706365017077,
        ),
        (32.311936193009416, 24.52975750457263, 34.27004696351677, 25.57686175475491),
        (
            132.69768680400875,
            -35.529851309592516,
            137.49438811692295,
            -30.5367373779689,
        ),
        (
            -27.463528785803362,
            11.714120500771458,
            -23.233545333100885,
            19.19829993688923,
        ),
        (
            -100.55187219599762,
            30.266852110518073,
            -98.56625317033243,
            33.49356030016899,
        ),
        (
            58.37467580639023,
            -28.000408828173292,
            60.972475656937874,
            -21.960679338099517,
        ),
        (
            -145.33534107232651,
            -5.840026833397943,
            -137.34755098957643,
            2.129135104657303,
        ),
        (
            -145.09135482612479,
            -40.15839628261435,
            -142.8962715495966,
            -32.685647196464046,
        ),
        (129.49381905338947, 53.097833947835994, 132.51308305442666, 54.44403392345813),
        (113.4732845775344, 28.49558951223193, 118.40553892676941, 36.39473071492081),
        (52.35194802164909, -68.90476499869789, 58.90707068906124, -66.43967285631805),
        (55.5521630884663, 61.450200549794545, 56.71306289217391, 62.46208704610545),
        (-133.60776757879802, 7.451309723874232, -131.35621670151096, 12.329465357413),
        (73.9881436271913, -41.49637627415659, 79.09862350238343, -39.31090345127635),
        (
            -3.8991702692119645,
            56.74710875110526,
            2.8850490658173937,
            57.576266646035855,
        ),
        (
            -25.98423732833305,
            -31.264768643884764,
            -25.85622638453957,
            -25.07292678202971,
        ),
        (46.61854828246908, -33.32626325919125, 52.57427245841773, -28.86798793229276),
        (-24.58644754656902, -68.6462420548324, -23.89202105198626, -61.56970154776127),
        (137.33571433036366, 6.38264048877312, 144.0290149874632, 11.084466064042493),
        (
            -119.64811290705589,
            -52.15762730050057,
            -117.11287194260784,
            -44.95567353943411,
        ),
        (100.68158366193416, 50.49836148012639, 107.8830882904933, 52.25796613301045),
        (-85.1598886642057, -55.60889296595001, -78.8969703534213, -48.52422882448704),
        (
            -31.831687457080278,
            16.892611421099787,
            -30.51071608425586,
            24.338671445079815,
        ),
        (123.96593671478774, 66.66884461033479, 130.47103330231687, 73.73203262717504),
        (
            -161.57263695461583,
            33.11902604571149,
            -158.84837175783903,
            40.572471545493265,
        ),
        (102.75994723862726, 50.968963972539115, 109.26486683956493, 53.17672907833544),
        (97.70733310606016, -54.866612303586, 104.69745069101828, -47.98372561801753),
        (-94.3725360344741, 44.32212478357006, -90.63614048051045, 46.833132635545006),
        (
            100.41746971197301,
            -38.136631762912145,
            100.70441874611448,
            -36.510906435123246,
        ),
        (
            -58.39093659277978,
            51.009411884240095,
            -50.65251267079773,
            53.314499326742876,
        ),
        (48.10379112659342, -14.045026189591482, 55.95487365545968, -9.708921903009578),
        (
            149.34062771040334,
            -53.85215474080014,
            157.10679253747895,
            -52.34146899303767,
        ),
        (
            157.26166735892883,
            -32.83470924678439,
            158.21804748192505,
            -29.301655554123702,
        ),
        (77.70532062191947, -26.085176012701226, 82.594370563038, -21.94493384131235),
        (
            -39.03355266279277,
            10.722326089523932,
            -36.921244864297975,
            16.42172983181388,
        ),
        (
            -169.42496540566597,
            59.58052316987158,
            -165.07119462863292,
            65.36402016311587,
        ),
        (82.26302646542203, 23.88799062061993, 85.24037609249402, 24.54078372851778),
        (55.840812869832604, -23.7719949540365, 58.42074646579365, -16.97267424593645),
        (74.71644942474308, -27.954882450423014, 77.25979825619143, -24.6285784723312),
        (
            -33.183868400373626,
            -28.60827164636742,
            -32.078294787806314,
            -25.186745609561445,
        ),
        (
            149.72364804826225,
            24.824512338182615,
            156.95581185954964,
            29.787080174198522,
        ),
        (-67.6770426477078, 6.711209838997746, -67.57383572409923, 9.077828202262271),
        (
            -23.838029003456228,
            11.19786936739547,
            -18.565854576201964,
            14.971276070346882,
        ),
    ]
    left_rows = [gm.box(*b, crs=4326) for b in _LEFT_BOXES]
    right_rows = [gm.box(*b, crs=4326) for b in _RIGHT_BOXES]
    left.insert(left_rows)
    right.insert(right_rows)

    # Build the left clone's lazy cap table, then query the divergent clone.
    # A shared cache used to return handle 17 here although handle 38 is over
    # 1,700 km closer.
    left.nearest(gm.Point(-170.0, -70.0, crs=4326))
    query = gm.Point(-170.0, -70.0, crs=4326)
    actual = int(right.nearest(query)[0])
    distances = [gm.distance(query, row, unit='meters') for row in right_rows]
    expected = min(
        range(len(distances)), key=lambda handle: (distances[handle], handle)
    )
    assert actual == expected == 38


def test_spatial_index_intersects_refines_crossing_candidates() -> None:
    values = [gm.LineString([(-2, 0), (2, 0)]), gm.LineString([(-2, 2), (2, 2)])]
    assert ids(
        gm.SpatialIndex(values).query(gm.box(-1, -1, 1, 1), predicate='intersects')
    ) == [0]


def test_spatial_index_uses_bounds_for_candidate_queries_in_input_order() -> None:
    values = [gm.Point(3, 3), gm.box(-1, -1, 1, 1), gm.LineString([(0, -2), (0, 2)])]
    idx = gm.SpatialIndex(values)
    area = gm.box(-2, -2, 2, 2)
    np.testing.assert_allclose(ids(idx.candidates(area)), [1, 2])
    np.testing.assert_allclose(ids(idx.query(area)), [1, 2])
    matches = idx.query(gm.GeometryArray([area]))
    assert matches.to_list() == [[1, 2]]


def test_spatial_index_query_defaults_to_exact_intersects_not_candidates() -> None:
    donut = gm.Polygon(
        [(0, 0), (4, 0), (4, 4), (0, 4)], holes=[[(1, 1), (3, 1), (3, 3), (1, 3)]]
    )
    idx = gm.SpatialIndex([donut])
    hole_point = gm.Point(2, 2)
    assert ids(idx.candidates(hole_point)) == [0]
    assert ids(idx.query(hole_point)) == []
    assert ids(idx.query(hole_point, predicate='intersects')) == []


def test_index_predicate_controls_are_keyword_only() -> None:
    point = gm.Point(0, 0)
    index = gm.SpatialIndex([point])
    with pytest.raises(TypeError, match='positional'):
        index.query(point, 'intersects')  # type: ignore[call-arg]
    with pytest.raises(TypeError, match='positional'):
        index.self_join('intersects')  # type: ignore[call-arg]
    with pytest.raises(TypeError, match='positional'):
        gm.join([point], [point], 'intersects')  # type: ignore[call-arg]


def test_spatial_index_explains_and_refines_dwithin_queries() -> None:
    values = [gm.Point(0, 0), gm.Point(2, 0), gm.LineString([(4, 0), (4, 4)])]
    idx = gm.SpatialIndex(values)
    query = gm.Point(1, 0)
    assert gm.distance(gm.Point(0, 0), query) == 1
    assert gm.dwithin(gm.Point(0, 0), query, 1)
    np.testing.assert_allclose(
        ids(idx.query(query, predicate='dwithin', distance=1.1)), [0, 1]
    )
    assert ids(
        idx.query(gm.box(3.5, 2, 3.7, 2.2), predicate='dwithin', distance=0.5)
    ) == [2]
    assert ids(idx.query(query, predicate='dwithin', distance=0.5)) == []
    np.testing.assert_allclose(ids(idx.candidates(query, distance=1.1)), [0, 1])
    assert ids(idx.candidates(query, distance=0.5)) == []
    assert idx.explain(query, predicate='dwithin', distance=1.1) == [
        'loaded 3 geometries',
        'bulk-loaded packed STR envelope index',
        'predicate operands: predicate(query_geom, indexed_row)',
        'bounds envelope candidate filter expanded by 1.1',
        'exact planar distance refine within 1.1',
    ]
    assert idx.explain(query) == [
        'loaded 3 geometries',
        'bulk-loaded packed STR envelope index',
        'bounds envelope candidate filter',
    ]
    assert (
        gm.SpatialIndex(gm.points([0.0], [0.0])).explain(gm.Point(0, 0))[0]
        == 'loaded 1 geometry'
    )
    assert (
        idx.explain(query, distance=1.1)[-1]
        == 'bounds envelope candidate filter expanded by 1.1'
    )
    with pytest.raises(gm.GeometryError, match='non-negative distance'):
        idx.query(query, predicate='dwithin')
    with pytest.raises(ValueError, match='unknown predicate'):
        idx.query(query, predicate=cast('Any', 'bogus'))
    with pytest.raises(gm.GeometryError, match='non-negative finite number'):
        idx.query(query, predicate='dwithin', distance=-1)
    with pytest.raises(ValueError, match="only valid with predicate='dwithin'"):
        idx.query(query, distance=5.0)
    lonlat = gm.points([21.0, 22.0], [52.0, 52.0], crs=4326)
    lonlat_index = gm.SpatialIndex(lonlat)
    lonlat_query = gm.Point(21.5, 52.0, crs=4326)
    assert ids(
        lonlat_index.query(
            lonlat_query, predicate='dwithin', distance=1.0, unit='planar'
        )
    ) == [0, 1]
    assert ids(lonlat_index.candidates(lonlat_query, distance=1.0, unit='planar')) == [
        0,
        1,
    ]
    assert (
        ids(lonlat_index.query(lonlat_query, predicate='dwithin', distance=1.0)) == []
    )
    assert (
        ids(
            lonlat_index.query(
                lonlat_query, predicate='dwithin', distance=1.0, unit='meters'
            )
        )
        == []
    )
    assert ids(lonlat_index.candidates(lonlat_query, distance=1.0)) == []
    np.testing.assert_allclose(
        ids(lonlat_index.candidates(lonlat_query, distance=100000.0)), [0, 1]
    )
    with pytest.raises(ValueError, match="expected 'planar' or 'meters'"):
        lonlat_index.query(
            lonlat_query, predicate='dwithin', distance=1.0, unit=cast('Any', 'km')
        )


def test_join_returns_exact_refined_index_pairs() -> None:
    points = gm.points([0.0, 2.0, 5.0], [0.0, 0.0, 0.0], crs=4326)
    polygons = gm.GeometryArray(
        [gm.box(-1, -1, 1, 1, crs=4326), gm.box(1, -1, 3, 1, crs=4326)], crs=4326
    )
    pairs = gm.join(points, polygons, predicate='within')
    assert isinstance(pairs, tuple)
    assert pair_rows(pairs) == [(0, 0), (1, 1)]


def test_join_supports_dwithin_and_scalar_inputs() -> None:
    left = [gm.Point(0, 0), gm.Point(4, 0)]
    right = gm.points([1.0, 2.0, 10.0], [0.0, 0.0, 0.0])
    assert pair_rows(gm.join(left, right, predicate='dwithin', distance=1.1)) == [
        (0, 0)
    ]
    assert pair_rows(
        gm.join(gm.Point(0, 0), gm.Point(0.5, 0), predicate='dwithin', distance=1)
    ) == [(0, 0)]
    lonlat = gm.points([21.0, 22.0], [52.0, 52.0], crs=4326)
    assert pair_rows(
        gm.join(
            gm.Point(21.5, 52.0, crs=4326),
            lonlat,
            predicate='dwithin',
            distance=1.0,
            unit='planar',
        )
    ) == [(0, 0), (0, 1)]
    assert (
        pair_rows(
            gm.join(
                gm.Point(21.5, 52.0, crs=4326),
                lonlat,
                predicate='dwithin',
                distance=1.0,
            )
        )
        == []
    )
    with pytest.raises(ValueError, match='requires a non-negative distance'):
        gm.join(left, right, predicate='dwithin')
    with pytest.raises(ValueError, match='unknown predicate'):
        gm.join(left, right, predicate=cast('Any', 'bogus'))


def test_join_skips_missing_right_rows_without_renumbering_handles() -> None:
    right = [gm.box(0, 0, 1, 1), None, gm.box(10, 10, 11, 11)]
    queries = gm.GeometryArray([gm.Point(0.5, 0.5), gm.Point(10.5, 10.5)])
    assert pair_rows(gm.join(queries, right, predicate='within')) == [(0, 0), (1, 2)]

    index = gm.SpatialIndex(gm.GeometryArray(right))
    assert len(index) == 2
    assert list(index) == [0, 2]
    assert pair_rows(index.join(queries, predicate='within')) == [(0, 0), (1, 2)]

    nonpoint_queries = gm.GeometryArray([
        gm.LineString([(0.25, 0.5), (0.75, 0.5)]),
        None,
        gm.LineString([(10.25, 10.5), (10.75, 10.5)]),
    ])
    assert pair_rows(index.join(nonpoint_queries)) == [(0, 0), (2, 2)]
    assert pair_rows(
        index.join(nonpoint_queries, predicate='dwithin', distance=0.0)
    ) == [(0, 0), (2, 2)]
    assert index.insert(gm.Point(20, 20)) == 3


def test_index_query_groups_expand_to_pair_columns() -> None:
    index = gm.SpatialIndex(gm.points([0, 1, 10], [0, 0, 0]))
    matches = index.query(
        gm.GeometryArray([gm.box(-1, -1, 2, 1), gm.box(50, 50, 51, 51)])
    )
    left, right = matches.to_pairs()
    assert left.tolist() == [0, 0]
    assert right.tolist() == [0, 1]
    assert left.dtype == right.dtype == np.int64
    assert not left.flags.writeable and not right.flags.writeable

    sliced_left, sliced_right = matches[1:].to_pairs()
    assert sliced_left.tolist() == []
    assert sliced_right.tolist() == []


def test_spatial_index_contains_refine_rejects_concave_false_positive() -> None:
    values = [gm.Polygon([(0, 0), (2, 0), (2, 2), (1, 1), (0, 2)]), gm.box(0, 0, 2, 2)]
    query = gm.LineString([(0.5, 1.5), (1.5, 1.5)])
    assert ids(gm.SpatialIndex(values).query(query, predicate='within')) == [1]


def test_geometry_array_prepared_contains_and_index() -> None:
    values = gm.points([0, 2, 0.5, 1], [0, 0, 0.5, 0], crs=4326)
    polygon = gm.box(-1, -1, 1, 1, crs=4326)
    assert len(values) == 4
    assert values.total_bounds == (0.0, 0.0, 2.0, 0.5)
    np.testing.assert_array_equal(
        gm.contains(polygon, values), [True, False, True, False]
    )
    assert gm.within(values[0], polygon)
    assert gm.covered_by(values[0], polygon)
    assert gm.covered_by(list(values)[3], polygon)
    np.testing.assert_array_equal(gm.covers(polygon, values), [True, False, True, True])
    np.testing.assert_array_equal(
        gm.intersects(polygon, values), [True, False, True, True]
    )
    hole = gm.Polygon(
        [(0, 0), (4, 0), (4, 4), (0, 4), (0, 0)],
        holes=[[(1, 1), (3, 1), (3, 3), (1, 3), (1, 1)]],
    )
    hole_points = gm.points([0, 2, 1, 2], [2, 2, 2, 0])
    np.testing.assert_array_equal(
        gm.contains(hole, hole_points), [False, False, False, False]
    )
    np.testing.assert_array_equal(
        gm.covers(hole, hole_points), [True, False, True, True]
    )
    np.testing.assert_array_equal(
        gm.intersects(hole, hole_points), [True, False, True, True]
    )
    np.testing.assert_array_equal(
        gm.disjoint(hole, hole_points), [False, True, False, False]
    )
    np.testing.assert_array_equal(
        gm.disjoint(polygon, values), [False, True, False, False]
    )
    np.testing.assert_array_equal(
        gm.equals(gm.Point(0, 0, crs=4326), values), [True, False, False, False]
    )
    prepared = polygon.prepare()
    np.testing.assert_array_equal(
        gm.contains(prepared, values), [True, False, True, False]
    )
    assert gm.contains_xy(prepared, 0, 0)
    family = (
        'contains',
        'intersects',
        'within',
        'covers',
        'covered_by',
        'disjoint',
        'touches',
        'crosses',
        'overlaps',
        'equals',
    )
    probes = [
        gm.box(-2, -2, 0, 0, crs=4326),
        gm.box(-0.5, -0.5, 0.5, 0.5, crs=4326),
        gm.Point(0, 0, crs=4326),
        gm.LineString([(-1, -1), (1, 1)], crs=4326),
        polygon,
    ]
    for predicate in family:
        np.testing.assert_array_equal(
            getattr(gm, predicate)(prepared, values),
            [getattr(gm, predicate)(polygon, item) for item in list(values)],
            err_msg=predicate,
        )
        np.testing.assert_array_equal(
            getattr(gm, predicate)(values, prepared),
            [getattr(gm, predicate)(item, polygon) for item in list(values)],
            err_msg=f'{predicate} right-prepared',
        )
        for probe in probes:
            assert getattr(gm, predicate)(prepared, probe) == getattr(gm, predicate)(
                polygon, probe
            ), (predicate, (probe).to_wkt())
    assert gm.dwithin(prepared, gm.Point(2, 0, crs=4326), 1.5) == gm.dwithin(
        polygon, gm.Point(2, 0, crs=4326), 1.5
    )
    np.testing.assert_array_equal(
        gm.dwithin(prepared, values, 0.0),
        [gm.dwithin(polygon, item, 0.0) for item in list(values)],
    )
    assert repr(prepared) == '<PreparedGeometry geometry_type=Polygon>'
    assert ids(gm.SpatialIndex(values).query(polygon, predicate='contains')) == [0, 2]
    assert ids(gm.SpatialIndex(values).query(polygon, predicate='covers')) == [0, 2, 3]
    assert (
        ids(gm.SpatialIndex([polygon]).query(list(values)[3], predicate='within')) == []
    )
    assert ids(
        gm.SpatialIndex([polygon]).query(list(values)[3], predicate='covered_by')
    ) == [0]
    assert ids(
        gm.SpatialIndex([polygon, gm.box(2, 2, 3, 3, crs=4326)]).query(
            polygon, predicate='equals'
        )
    ) == [0]
    assert ids(
        gm.SpatialIndex(values).nearest(gm.Point(1.8, 0, crs=4326), k=2, unit='planar')
    ) == [1, 3]


def test_spatial_index_nearest_branch_and_bound_matches_brute_force() -> None:
    geoms = [
        gm.box(i * 3, j * 3, i * 3 + 1, j * 3 + 1) for i in range(6) for j in range(6)
    ]
    index = gm.SpatialIndex(geoms)
    for qx, qy in [(1.2, 1.2), (7.5, 4.0), (16.0, 9.3), (-2.0, -2.0)]:
        query = gm.box(qx, qy, qx + 0.5, qy + 0.5)
        for k in (1, 3, 5):
            got = index.nearest(query, k=k)
            got_distances = sorted(gm.distance(query, geoms[i]) for i in got)
            want_distances = sorted(gm.distance(query, g) for g in geoms)[:k]
            assert got_distances == pytest.approx(want_distances), (qx, qy, k)
    probe = gm.box(0, 0, 0.5, 0.5)
    near = index.nearest(probe, k=10, max_distance=1.5)
    assert all(cast('float', gm.distance(probe, geoms[i])) <= 1.5 for i in near)


def test_spatial_index_completeness_len_batch_pairs_and_nearest_distance() -> None:
    points = gm.points([0, 1, 2, 3, 10], [0, 0, 0, 0, 0])
    index = gm.SpatialIndex(points)
    assert len(index) == 5
    queries = gm.GeometryArray([gm.box(-0.5, -0.5, 1.5, 0.5), gm.box(9, -1, 11, 1)])
    assert ids(index.query(queries)) == [[0, 1], [4]]
    boxes = gm.GeometryArray([
        gm.box(0, 0, 2, 2),
        gm.box(1, 1, 3, 3),
        gm.box(5, 5, 6, 6),
    ])
    assert pair_rows(gm.SpatialIndex(boxes).self_join()) == [(0, 1)]
    assert pair_rows(gm.SpatialIndex(boxes).self_join(predicate='overlaps')) == [(0, 1)]
    probe = gm.Point(1.1, 0)
    assert ids(index.nearest(probe, k=2)) == [1, 2]
    indices, distances = index.nearest(probe, k=1, return_distance=True)
    assert ids(indices) == [1]
    assert floats(distances) == pytest.approx([0.1])
    np.testing.assert_allclose(ids(index.nearest(probe, k=5, max_distance=0.5)), [1])
    indices, distances = index.nearest(
        probe, k=5, max_distance=0.5, return_distance=True
    )
    assert ids(indices) == [1]
    assert floats(distances) == pytest.approx([0.1])
    matches, batch_distances = index.nearest(queries, k=1, return_distance=True)
    assert ids(matches) == ids(index.nearest(queries, k=1))
    assert len(batch_distances) == len(matches.values)
    assert index.remove(4)
    assert len(index) == 4
    assert ids(index.query(gm.box(9, -1, 11, 1))) == []
    with pytest.raises(ValueError, match='max_distance'):
        index.nearest(probe, max_distance=-1.0)


def test_spatial_index_nearest_refines_exact_planar_distance() -> None:
    values = [
        gm.Polygon(
            [(-10, -10), (10, -10), (10, 10), (-10, 10)],
            holes=[[(-9, -9), (9, -9), (9, 9), (-9, 9)]],
        ),
        gm.Point(0, 2),
        gm.Point(0, 20),
    ]
    idx = gm.SpatialIndex(values)
    query = gm.Point(0, 0)
    assert values[0].bounds == (-10.0, -10.0, 10.0, 10.0)
    assert gm.distance(values[0], query) == 9
    assert gm.distance(values[1], query) == 2
    assert ids(idx.nearest(query, k=2)) == [1, 0]
    assert ids(idx.nearest(gm.points([0, 0], [0, 20]), k=1)) == [[1], [2]]
    assert ids(gm.nearest(values, query, k=2)) == [1, 0]
    assert ids(gm.nearest(values, gm.points([0, 0], [0, 20]), k=1)) == [[1], [2]]
    assert ids(idx.nearest(query, k=0)) == []


def test_nearest_can_use_geodesic_meter_units_for_points() -> None:
    values = gm.points([0, 10], [82, 80], crs=4326)
    query = gm.Point(0, 80, crs=4326)
    queries = gm.points([0, 0], [80, 82], crs=4326)
    assert ids(gm.nearest(values, query, unit='planar')) == [0]
    assert ids(gm.nearest(values, query, unit='meters')) == [1]
    assert ids(gm.SpatialIndex(values).nearest(query, unit='meters')) == [1]
    assert ids(gm.nearest(values, queries, unit='meters')) == [[1], [0]]
    assert ids(gm.nearest(values, query)) == [1]
    assert ids(gm.SpatialIndex(values).nearest(query)) == [1]
    assert ids(gm.nearest([gm.box(0, 0, 1, 1, crs=4326)], query, unit='meters')) == [0]
    with pytest.raises(ValueError, match="expected 'planar' or 'meters'"):
        gm.nearest(values, query, unit=cast('Any', 'km'))


def test_spatial_index_mutable_insert_and_remove() -> None:
    index = gm.SpatialIndex([gm.box(0, 0, 1, 1), gm.box(5, 5, 6, 6)])
    assert not hasattr(index, 'insert_many')
    handle = index.insert(gm.box(10, 10, 11, 11))
    assert isinstance(handle, int)
    assert handle == 2
    assert handle in index.query(gm.box(9, 9, 12, 12))
    assert index.remove(0) is True
    assert 0 not in index.query(gm.box(-1, -1, 2, 2))
    assert 1 in index.query(gm.box(4, 4, 7, 7))
    assert index.remove(0) is False
    assert index.remove(999) is False
    with pytest.raises(ValueError, match='spatial index handle must be non-negative'):
        index.remove(-1)
    with pytest.raises(ValueError, match='spatial index handle is too large'):
        index.remove(2**80)


def test_spatial_index_read_only_mapping_views_and_lazy_iterator() -> None:
    from collections.abc import Mapping
    from operator import length_hint

    index = gm.SpatialIndex([gm.Point(0, 0), None, gm.Point(2, 2)])
    assert isinstance(index, Mapping)
    iterator = iter(index)
    assert type(iterator).__name__ == 'SpatialIndexIterator'
    assert length_hint(iterator) == 2
    assert next(iterator) == 0
    assert length_hint(iterator) == 1
    assert next(iterator) == 2
    with pytest.raises(StopIteration):
        next(iterator)

    assert list(index.keys()) == [0, 2]
    assert [geom.to_wkt() for geom in index.values()] == ['POINT (0 0)', 'POINT (2 2)']
    assert [(key, geom.to_wkt()) for key, geom in index.items()] == [
        (0, 'POINT (0 0)'),
        (2, 'POINT (2 2)'),
    ]
    assert index.get(0).to_wkt() == 'POINT (0 0)'
    marker = object()
    assert index.get(1) is None
    assert index.get(99, marker) is marker

    invalidated = iter(index)
    index.insert(gm.Point(3, 3))
    with pytest.raises(RuntimeError, match='changed during iteration'):
        next(invalidated)


def test_spatial_index_iterator_size_tracks_retained_index() -> None:
    import sys

    small = gm.SpatialIndex([gm.box(0, 0, 1, 1)])
    large = gm.SpatialIndex([gm.box(i, 0, i + 1, 1) for i in range(256)])
    small_iterator = iter(small)
    large_iterator = iter(large)

    small_size = sys.getsizeof(small_iterator)
    assert small_size > 0
    assert sys.getsizeof(large_iterator) > small_size

    assert next(small_iterator) == 0
    assert sys.getsizeof(small_iterator) == small_size


def test_spatial_index_insert_array_matches_insert_loop() -> None:
    base = [gm.box(0, 0, 1, 1)]
    additions = gm.GeometryArray([
        gm.Point(2, 2),
        gm.box(4, 4, 5, 5),
        gm.LineString([(8, 8), (9, 9)]),
    ])
    batch = gm.SpatialIndex(base)
    loop = gm.SpatialIndex(base)
    handles = batch.insert(additions)
    assert isinstance(handles, np.ndarray)
    assert handles.dtype == np.int64
    assert handles.tolist() == [1, 2, 3]
    assert [loop.insert(geom) for geom in additions] == [1, 2, 3]
    query = gm.box(-1, -1, 10, 10)
    assert ids(batch.query(query)) == ids(loop.query(query))
    assert pair_rows(batch.self_join(predicate='intersects')) == pair_rows(
        loop.self_join(predicate='intersects')
    )


def test_spatial_index_insert_rejects_frame_mismatch() -> None:
    idx = gm.SpatialIndex(gm.points([0.0], [0.0], crs=4326))
    before = ids(idx.query(gm.box(-1, -1, 1, 1, crs=4326)))
    with pytest.raises(gm.CRSMismatchError, match='share the index CRS'):
        idx.insert(gm.points([0.0, 1.0], [0.0, 1.0], crs=3857))
    assert ids(idx.query(gm.box(-1, -1, 1, 1, crs=4326))) == before


def test_insert_array_antimeridian_crossing_row_is_self_join_visible() -> None:
    box175 = gm.box(174.0, -5.0, 176.0, 5.0, crs=4326)
    idx = gm.SpatialIndex(gm.GeometryArray([box175]))
    crossing = gm.GeometryArray([
        gm.Polygon(
            [
                (170.0, -6.0),
                (-170.0, -6.0),
                (-170.0, 6.0),
                (170.0, 6.0),
                (170.0, -6.0),
            ],
            crs=4326,
        )
    ])
    assert idx.insert(crossing) == [1]
    assert pair_rows(idx.self_join(predicate='intersects')) == [(0, 1)]


def test_insert_iterable_path_widens_antimeridian_crossing() -> None:
    # The Python-list (iterable) path must widen crossing envelopes just like the
    # packed-array path, or self_join silently misses the inserted pair.
    box175 = gm.box(174.0, -5.0, 176.0, 5.0, crs=4326)
    idx = gm.SpatialIndex(gm.GeometryArray([box175]))
    crossing = gm.Polygon(
        [(170.0, -6.0), (-170.0, -6.0), (-170.0, 6.0), (170.0, 6.0), (170.0, -6.0)],
        crs=4326,
    )
    assert idx.insert([crossing]) == [1]
    assert pair_rows(idx.self_join(predicate='intersects')) == [(0, 1)]


def test_insert_batch_is_atomic_on_a_bad_row() -> None:
    # A bad row (empty geometry) mid-batch must roll back wholly: no rows
    # inserted, no handles consumed, index unchanged.
    idx = gm.SpatialIndex(gm.GeometryArray([gm.Point(0.0, 0.0)]))
    before = ids(idx.query(gm.box(-1, -1, 10, 10)))
    with pytest.raises(gm.GeometryError, match='cannot index empty geometry'):
        idx.insert([gm.Point(1.0, 1.0), gm.from_wkt('POINT EMPTY'), gm.Point(2.0, 2.0)])
    assert len(idx) == 1
    assert ids(idx.query(gm.box(-1, -1, 10, 10))) == before
    # handles were not consumed — the next single insert still gets handle 1
    assert idx.insert(gm.Point(3.0, 3.0)) == 1


def test_insert_missing_array_is_atomic_and_does_not_consume_handles() -> None:
    idx = gm.SpatialIndex([gm.Point(0.0, 0.0)])
    before = ids(idx.query(gm.box(-1, -1, 10, 10)))
    values = gm.GeometryArray([gm.Point(1.0, 1.0), None, gm.Point(2.0, 2.0)])

    with pytest.raises(gm.GeometryError, match='containing missing geometries'):
        idx.insert(values)

    assert len(idx) == 1
    assert ids(idx.query(gm.box(-1, -1, 10, 10))) == before
    handle = idx.insert(gm.Point(3.0, 3.0))
    assert handle == 1
    assert idx.remove(handle)


def test_scalar_insert_of_empty_into_fresh_index_does_not_frame_lock() -> None:
    # A failed empty scalar insert must NOT adopt/lock a frameless index's frame:
    # the envelope (which rejects the empty geometry) is built BEFORE the frame is
    # adopted, so a later valid insert in a different CRS still succeeds.
    idx = gm.SpatialIndex(gm.GeometryArray([]))
    with pytest.raises(gm.GeometryError, match='cannot index empty geometry'):
        idx.insert(gm.from_wkt('POLYGON EMPTY', crs=4326))
    assert idx.insert(gm.Point(1.0, 1.0, crs=3857)) == 0  # not frame-locked to 4326


def test_packed_array_index_mutates_like_a_boxed_one() -> None:
    packed = gm.points([0.0, 1.0, 2.0], [0.0, 1.0, 2.0])
    index = gm.SpatialIndex(packed)
    assert ids(index.nearest(gm.Point(0.1, 0.1))) == [0]
    assert index.insert(gm.Point(5.0, 5.0)) == 3
    assert index.insert(gm.box(10, 10, 11, 11)) == 4
    assert ids(index.query(gm.box(-1, -1, 20, 20))) == [0, 1, 2, 3, 4]
    assert ids(index.nearest(gm.Point(4.9, 4.9))) == [3]
    assert index.remove(1) is True
    assert index.remove(1) is False
    assert index.remove(3) is True
    assert ids(index.query(gm.box(-1, -1, 20, 20))) == [0, 2, 4]
    assert ids(index.nearest(gm.Point(4.9, 4.9))) == [2]
    pairs_index = gm.SpatialIndex(gm.points([0.0, 0.5, 9.0], [0.0, 0.5, 9.0]))
    pairs_index.insert(gm.Point(0.25, 0.25))
    assert pair_rows(pairs_index.self_join(predicate='dwithin', distance=1.0)) == [
        (0, 1),
        (0, 3),
        (1, 3),
    ]
    geographic = gm.SpatialIndex(gm.points([10.0, 11.0], [50.0, 50.5], crs=4326))
    with pytest.raises(gm.CRSMismatchError, match='share the index CRS'):
        geographic.query(gm.Point(10.0, 50.1))


def test_spatial_index_remove_excludes_removed_from_nearest() -> None:
    polys = [gm.box(x, 0, x + 1, 1) for x in (0, 10, 20)]
    index = gm.SpatialIndex(polys)
    query = gm.box(0.1, 0, 1.1, 1)
    assert ids(index.nearest(query)) == [0]
    assert index.remove(0) is True
    after = index.nearest(query)
    assert 0 not in after, f'removed geometry still won nearest: {after}'
    assert ids(after) == [1]


def test_self_join_rejects_directional_predicates() -> None:
    idx = gm.SpatialIndex([gm.box(0, 0, 4, 4), gm.box(1, 1, 2, 2)])
    assert pair_rows(idx.self_join(predicate='intersects')) == [(0, 1)]
    with pytest.raises(ValueError, match='symmetric'):
        idx.self_join(predicate=cast('Any', 'contains'))


def test_geodesic_nearest_pruning_matches_brute_force_on_adversarial_points() -> None:
    """The reduced-latitude lower bound prunes without ever changing results.

    The point set deliberately includes the bound's adversarial cases:
    equatorial micro-deltas (where the naive geodetic ``b*sigma`` bound is
    unsound), polar clusters, antimeridian straddles, and a near-antipodal
    pair. The pruned traversal must agree exactly with a brute-force scan.
    """
    micro = [
        (base + k * 1e-5, (k - 4) * 1e-5)
        for base in (-170.0, -10.0, 0.0, 10.0, 170.0)
        for k in (0, 1, 4, 7)
    ]
    polar = [
        (lon, lat) for lat in (89.99, -89.99) for lon in (-150.0, -60.0, 30.0, 120.0)
    ]
    seam = [(179.999, 10.0), (-179.999, 10.0), (179.99, -45.0), (-179.99, -45.0)]
    global_points = [
        (-135.0, -60.0),
        (-90.0, -20.0),
        (-45.0, 45.0),
        (45.0, -45.0),
        (90.0, 20.0),
        (135.0, 60.0),
        (180.0, 0.0),
        (-135.0, 45.0),
    ]
    points = micro + polar + seam + global_points
    values = gm.points(*zip(*points, strict=True), crs=4326)
    idx = gm.SpatialIndex(values)
    items = list(values)
    queries = [
        gm.Point(0.0, 0.0, crs=4326),
        gm.Point(0.0, 1e-05, crs=4326),
        gm.Point(179.9999, -10.0, crs=4326),
        gm.Point(-90.0, 89.999, crs=4326),
        gm.Point(45.0, -45.0, crs=4326),
    ]
    for query in queries:
        got_ids, got_distances = idx.nearest(query, k=5, return_distance=True)
        brute = sorted(
            ((idx_, gm.distance(query, item)) for idx_, item in enumerate(items)),
            key=lambda pair: (pair[1], pair[0]),
        )[:5]
        assert ids(got_ids) == [i for i, _ in brute]
        for a, b in zip(got_distances, (d for _, d in brute), strict=True):
            assert a == pytest.approx(b, abs=1e-09)


def test_geodesic_dwithin_window_matches_global_scan() -> None:
    points = [
        (0, 0),
        (1, 1),
        (3, 0),
        (0, 4),
        (5, 0),
        (10, 0),
        (179.5, 40),
        (-179.5, 40),
        (175, 40),
        (179.5, 45),
        (-175, 42),
        (160, 40),
        (10, 89.5),
        (0, 89),
        (90, 89),
        (-170, 89.9),
        (10, 85),
        (10, 80),
        (-60, -30),
        (-59.7, -30),
        (-60, -29.6),
        (-59, -30),
        (-60, -31),
        (-70, -30),
    ]
    values = gm.points(*zip(*points, strict=True), crs=4326)
    idx = gm.SpatialIndex(values)
    items = list(values)
    for query, radius, expected_count in [
        (gm.Point(0.0, 0.0, crs=4326), 500000.0, 4),
        (gm.Point(179.5, 40.0, crs=4326), 800000.0, 5),
        (gm.Point(10.0, 89.5, crs=4326), 300000.0, 4),
        (gm.Point(-60.0, -30.0, crs=4326), 50000.0, 3),
    ]:
        got = idx.query(query, predicate='dwithin', distance=radius)
        brute = [
            i for i, item in enumerate(items) if gm.distance(query, item) <= radius
        ]
        assert len(brute) == expected_count
        assert ids(got) == brute


def test_nearest_exclusive_skips_structurally_equal_geometries() -> None:
    pts = gm.points([0.0, 0.0, 1.0, 2.0], [0.0, 0.0, 0.0, 0.0], crs=4326)
    idx = gm.SpatialIndex(pts)
    probe = gm.Point(0.0, 0.0, crs=4326)
    assert ids(idx.nearest(probe, k=3)) == [0, 1, 2]
    assert ids(idx.nearest(probe, k=3, exclusive=True)) == [2, 3]
    assert ids(idx.nearest(probe, k=2, exclusive=True)) == [2, 3]
    assert ids(gm.nearest(pts, probe, k=2, exclusive=True)) == [2, 3]
    plain = gm.SpatialIndex(gm.points([0.0, 1.0], [0.0, 0.0]))
    assert ids(plain.nearest(gm.Point(0, 0), k=1, exclusive=True)) == [1]


def test_candidates_accepts_geometry_arrays() -> None:
    boxes = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(10, 10, 11, 11)])
    idx = gm.SpatialIndex(boxes)
    queries = gm.GeometryArray([gm.box(0.5, 0.5, 2, 2), gm.box(50, 50, 60, 60)])
    assert match_rows(idx.candidates(queries)) == [[0], []]
    assert ids(idx.query(queries)) == [[0], []]


def test_self_join_refines_each_unordered_pair_once_with_exact_results() -> None:
    boxes = gm.GeometryArray([
        gm.box(0, 0, 2, 2),
        gm.box(1, 1, 3, 3),
        gm.box(2.5, 2.5, 4, 4),
        gm.box(10, 10, 11, 11),
    ])
    idx = gm.SpatialIndex(boxes)
    pairs = idx.self_join(predicate='intersects')
    items = list(boxes)
    brute = [
        (i, j)
        for i in range(len(items))
        for j in range(i + 1, len(items))
        if gm.intersects(items[i], items[j])
    ]
    assert pair_rows(pairs) == brute


def test_index_rejects_disjoint_with_guidance() -> None:
    idx = gm.SpatialIndex([gm.box(0, 0, 1, 1)])
    with pytest.raises(ValueError, match='cannot be index-accelerated'):
        idx.query(gm.Point(0, 0), predicate=cast('Any', 'disjoint'))


def test_explain_validates_the_query_frame_and_reports_real_plans() -> None:
    idx = gm.SpatialIndex(gm.points([0.0, 1.0], [0.0, 0.0], crs=4326))
    with pytest.raises(gm.CRSMismatchError, match='share the index CRS'):
        idx.explain(gm.Point(0, 0, crs=3857))
    plan = idx.explain()
    assert plan == ['loaded 2 geometries', 'bulk-loaded packed STR envelope index']
    plan = idx.explain(
        gm.Point(0.5, 0.0, crs=4326), predicate='dwithin', distance=1000.0
    )
    assert 'predicate operands: predicate(query_geom, indexed_row)' in plan
    assert 'geodesic lower-bound candidate window' in plan
    assert any('exact CRS-aware distance refine' in step for step in plan)
    candidate_plan = idx.explain(gm.Point(0.5, 0.0, crs=4326), distance=1000.0)
    assert candidate_plan[-1] == 'geodesic lower-bound candidate window'


def test_join_packed_point_fast_path_matches_per_row_join() -> None:
    values = [
        (1, 1),
        (3.5, 3.5),
        (5, 5),
        (9.75, 9.75),
        (8.25, 8.25),
        (4.25, 2),
        (2, 4.25),
        (8.5, 5),
        (9.25, 9.75),
        (0, 0),
        (10, 10),
        (6, 9),
    ]
    points = gm.points(*zip(*values, strict=True))
    polygons = gm.GeometryArray([
        gm.box(0, 0, 4, 4),
        gm.box(3, 3, 8, 8),
        gm.box(9.5, 9.5, 10, 10),
    ])
    fast = gm.join(points, polygons, predicate='within')
    slow = gm.join(list(points), polygons, predicate='within')
    assert pair_rows(fast) == pair_rows(slow)
    assert pair_rows(
        gm.join(points, polygons, predicate='dwithin', distance=0.5)
    ) == pair_rows(gm.join(list(points), polygons, predicate='dwithin', distance=0.5))


def test_join_packed_nonpoint_left_lanes_match_per_row_join() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (2.0, 0.0)]),
        gm.LineString([(5.0, 5.0), (6.0, 6.0)]),
        gm.LineString([(1.0, 1.0), (1.0, 3.0)]),
    ])
    polygons = gm.GeometryArray([
        gm.box(-1.0, -1.0, 3.0, 1.0),
        gm.box(0.5, 0.5, 1.5, 3.5),
        gm.box(10.0, 10.0, 11.0, 11.0),
    ])
    assert pair_rows(gm.join(lines, polygons, predicate='intersects')) == pair_rows(
        gm.join(list(lines), polygons, predicate='intersects')
    )
    assert pair_rows(
        gm.join(polygons, lines, predicate='dwithin', distance=0.25)
    ) == pair_rows(gm.join(list(polygons), lines, predicate='dwithin', distance=0.25))


def test_geodesic_dwithin_window_covers_the_antimeridian_seam() -> None:
    query = gm.Point(-180.0, 0.0, crs=4326)
    entry = gm.Point(180.0, 0.0, crs=4326)
    assert gm.dwithin(query, entry, 0.0)
    np.testing.assert_allclose(
        ids(gm.SpatialIndex([entry]).query(query, predicate='dwithin', distance=0.0)),
        [0],
    )


def test_geodesic_index_paths_raise_the_same_domain_errors_as_scalars() -> None:
    idx = gm.SpatialIndex(gm.points([0.0], [0.0], crs=4326))
    bad_query = gm.Point(0.0, 95.0, crs=4326)
    with pytest.raises(
        gm.InvalidGeometryError,
        match=r'invalid longitude/latitude \(0, 95\); coordinates are \(x, y\) = \(lon, lat\) — use swap_xy\(\) for latitude-first data',
    ):
        idx.query(bad_query, predicate='dwithin', distance=1.0)
    with pytest.raises(
        gm.InvalidGeometryError,
        match=r'invalid longitude/latitude \(0, 95\); coordinates are \(x, y\) = \(lon, lat\) — use swap_xy\(\) for latitude-first data',
    ):
        idx.nearest(bad_query)
    bad_index = gm.SpatialIndex([gm.Point(0.0, 95.0, crs=4326)])
    with pytest.raises(
        gm.InvalidGeometryError,
        match=r'invalid longitude/latitude \(0, 95\); coordinates are \(x, y\) = \(lon, lat\) — use swap_xy\(\) for latitude-first data',
    ):
        bad_index.query(gm.Point(0.0, 0.0, crs=4326), predicate='dwithin', distance=1.0)


def test_exact_query_and_join_reject_a_stray_distance_argument() -> None:
    idx = gm.SpatialIndex([gm.box(0, 0, 1, 1)])
    with pytest.raises(ValueError, match="only valid with predicate='dwithin'"):
        idx.query(gm.Point(0, 0), predicate='intersects', distance=5.0)
    with pytest.raises(ValueError, match="only valid with predicate='dwithin'"):
        gm.join(
            [gm.Point(0, 0)],
            [gm.box(0, 0, 1, 1)],
            predicate='within',
            distance=5.0,
        )
    np.testing.assert_allclose(ids(idx.candidates(gm.Point(5, 5), distance=10.0)), [0])


def test_packed_point_query_lane_matches_per_row_queries() -> None:
    values = [
        (1, 1),
        (3.5, 3.5),
        (5, 5),
        (8.5, 8.5),
        (9.5, 9.5),
        (0, 0),
        (4, 4),
        (3, 3),
        (9, 9),
        (2.9, 4.1),
        (6, 2),
        (10, 10),
    ]
    xs, ys = zip(*values, strict=True)
    packed = gm.points(xs, ys, crs=4326)
    mixed = gm.from_wkt([(point).to_wkt() for point in packed], crs=packed.crs)
    idx = gm.SpatialIndex(
        gm.GeometryArray([gm.box(0, 0, 4, 4, crs=4326), gm.box(3, 3, 9, 9, crs=4326)])
    )
    assert idx.query(packed, predicate='within') == idx.query(mixed, predicate='within')
    assert idx.query(packed) == idx.query(mixed)
    assert idx.candidates(packed) == idx.candidates(mixed)
    geo_idx = gm.SpatialIndex(gm.points(xs, ys, crs=4326))
    assert geo_idx.query(
        packed, predicate='dwithin', distance=100000.0
    ) == geo_idx.query(mixed, predicate='dwithin', distance=100000.0)
    plan = geo_idx.explain(packed[0], predicate='dwithin', distance=1000.0)
    assert any('geodesic lower-bound candidate window' in step for step in plan)


def test_empty_index_answers_every_method_gracefully() -> None:
    idx = gm.SpatialIndex([])
    probe = gm.Point(0, 0)
    assert len(idx) == 0
    assert ids(idx.query(probe)) == []
    assert ids(idx.nearest(probe)) == []
    assert ids(idx.candidates(probe)) == []
    assert pair_rows(idx.self_join()) == []
    assert ids(idx.query(gm.Point(0, 0, crs=3857))) == []


def test_empty_geometry_queries_return_nothing() -> None:
    idx = gm.SpatialIndex([gm.box(0, 0, 1, 1)])
    empty = gm.from_wkt('POINT EMPTY')
    assert ids(idx.query(empty)) == []
    assert ids(idx.nearest(empty)) == []
    assert ids(idx.candidates(empty)) == []


def test_nearest_edge_parameters() -> None:
    idx = gm.SpatialIndex(gm.points([1.0, -1.0, 0.0], [0.0, 0.0, 1.0]))
    probe = gm.Point(0, 0)
    assert ids(idx.nearest(probe, k=0)) == []
    assert ids(idx.nearest(probe, k=99)) == [0, 1, 2]
    assert ids(idx.nearest(probe, k=2)) == [0, 1]
    np.testing.assert_allclose(ids(idx.nearest(gm.Point(1, 0), max_distance=0.0)), [0])
    assert ids(idx.nearest(probe, max_distance=0.5)) == []


def test_insert_of_a_polygon_disables_geodesic_pruning_without_changing_results() -> (
    None
):
    idx = gm.SpatialIndex(gm.points([0.0, 10.0], [0.0, 0.0], crs=4326))
    probe = gm.Point(1.0, 0.0, crs=4326)
    before = idx.nearest(probe, k=2, return_distance=True)
    idx.insert(gm.box(50, 50, 51, 51, crs=4326))
    after = idx.nearest(probe, k=2, return_distance=True)
    assert ids(before[0]) == ids(after[0])
    assert floats(before[1]) == floats(after[1])
    np.testing.assert_allclose(
        ids(idx.query(probe, predicate='dwithin', distance=200000.0)), [0]
    )


def test_remove_restores_geodesic_pruning_after_non_point_removed() -> None:
    idx = gm.SpatialIndex(gm.points([0.0, 10.0], [0.0, 0.0], crs=4326))
    probe = gm.Point(0.5, 0.0, crs=4326)
    pruned = 'geodesic lower-bound candidate window'
    full_scan = 'global candidate scan (exact geodesic distances)'
    plan_before = idx.explain(probe, predicate='dwithin', distance=1000.0)
    assert pruned in plan_before
    poly_handle = idx.insert(gm.box(50, 50, 51, 51, crs=4326))
    plan_degraded = idx.explain(probe, predicate='dwithin', distance=1000.0)
    assert full_scan in plan_degraded
    assert idx.remove(poly_handle)
    plan_restored = idx.explain(probe, predicate='dwithin', distance=1000.0)
    assert pruned in plan_restored


def test_remove_bulk_non_point_restores_geodesic_pruning() -> None:
    idx = gm.SpatialIndex([
        gm.Point(0.0, 0.0, crs=4326),
        gm.box(50.0, 50.0, 51.0, 51.0, crs=4326),
        gm.Point(10.0, 0.0, crs=4326),
    ])
    probe = gm.Point(0.5, 0.0, crs=4326)
    full_scan = 'global candidate scan (exact geodesic distances)'
    pruned = 'geodesic lower-bound candidate window'
    assert full_scan in idx.explain(probe, predicate='dwithin', distance=1000.0)
    assert idx.remove(1)
    assert pruned in idx.explain(probe, predicate='dwithin', distance=1000.0)


def test_remove_restores_pruning_only_after_every_non_point_is_gone() -> None:
    idx = gm.SpatialIndex(gm.points([0.0, 10.0], [0.0, 0.0], crs=4326))
    first = idx.insert(gm.box(20.0, 20.0, 21.0, 21.0, crs=4326))
    second = idx.insert(gm.LineString([(30.0, 30.0), (31.0, 31.0)], crs=4326))
    probe = gm.Point(0.5, 0.0, crs=4326)
    full_scan = 'global candidate scan (exact geodesic distances)'
    pruned = 'geodesic lower-bound candidate window'
    assert full_scan in idx.explain(probe, predicate='dwithin', distance=1000.0)
    assert idx.remove(first)
    assert full_scan in idx.explain(probe, predicate='dwithin', distance=1000.0)
    assert idx.remove(second)
    assert pruned in idx.explain(probe, predicate='dwithin', distance=1000.0)


def test_geodesic_nearest_max_distance_interacts_with_pruning() -> None:
    idx = gm.SpatialIndex(gm.points([0.0, 1.0, 50.0], [0.0, 0.0, 0.0], crs=4326))
    probe = gm.Point(0.5, 0.0, crs=4326)
    np.testing.assert_allclose(
        ids(idx.nearest(probe, k=3, max_distance=200000.0)), [0, 1]
    )
    assert ids(idx.nearest(probe, k=3, max_distance=1.0)) == []


def test_self_join_supports_geodesic_dwithin() -> None:
    idx = gm.SpatialIndex(gm.points([0.0, 1.0, 50.0], [0.0, 0.0, 0.0], crs=4326))
    np.testing.assert_allclose(
        pair_rows(idx.self_join(predicate='dwithin', distance=150000.0)), [(0, 1)]
    )


def test_index_entries_at_domain_corners_stay_searchable() -> None:
    corners = gm.points([-180.0, 180.0, 0.0, 0.0], [0.0, 0.0, 90.0, -90.0], crs=4326)
    idx = gm.SpatialIndex(corners)
    for item in list(corners):
        np.testing.assert_allclose(
            floats(idx.nearest(item, k=1, return_distance=True)[1]), [0.0]
        )
    seam_ids, seam_distances = idx.nearest(
        gm.Point(-180.0, 0.0, crs=4326), k=2, return_distance=True
    )
    assert ids(seam_ids) == [0, 1]
    np.testing.assert_allclose(floats(seam_distances), [0.0, 0.0])
    assert ids(idx.nearest(gm.Point(45.0, 90.0, crs=4326), k=1)) == [2]
    assert ids(idx.nearest(gm.Point(45.0, 89.0, crs=4326), k=1)) == [2]


def test_geodesic_full_scan_arm_matches_brute_force_for_shapes() -> None:
    items = gm.GeometryArray(
        [
            gm.LineString([(179.0, 10.0), (-179.0, 10.0)], crs=4326),
            gm.box(179.5, 9.5, 180.0, 10.5, crs=4326),
            gm.Point(0.0, 0.0, crs=4326),
        ],
        crs=4326,
    )
    idx = gm.SpatialIndex(items)
    rows = list(items)
    for query in (gm.Point(180.0, 10.0, crs=4326), gm.Point(170.0, 12.0, crs=4326)):
        got_ids, _got_distances = idx.nearest(query, k=3, return_distance=True)
        brute = sorted(
            ((i, gm.distance(query, item)) for i, item in enumerate(rows)),
            key=lambda pair: (pair[1], pair[0]),
        )
        assert ids(got_ids) == [i for i, _ in brute]
        for radius in (1.0, 20000.0, 500000.0):
            assert ids(idx.query(query, predicate='dwithin', distance=radius)) == [
                i for i, item in enumerate(rows) if gm.dwithin(query, item, radius)
            ]


def test_mutation_keeps_handles_stable_through_self_join() -> None:
    idx = gm.SpatialIndex([
        gm.box(0, 0, 2, 2),
        gm.box(1, 1, 3, 3),
        gm.box(2.5, 2.5, 4, 4),
    ])
    assert pair_rows(idx.self_join(predicate='intersects')) == [(0, 1), (1, 2)]
    assert idx.remove(1)
    assert pair_rows(idx.self_join(predicate='intersects')) == []
    assert idx.insert(gm.box(0.5, 0.5, 1.5, 1.5)) == 3
    assert pair_rows(idx.self_join(predicate='intersects')) == [(0, 3)]
    assert not idx.remove(1)


def test_mixed_storage_geodesic_array_queries_take_the_per_row_path() -> None:
    idx = gm.SpatialIndex(gm.points([0.0, 0.2, 10.0], [0.0, 0.0, 0.0], crs=4326))
    queries = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        gm.LineString([(0.0, 0.0), (0.1, 0.0)], crs=4326),
    ])
    assert ids(idx.query(queries, predicate='dwithin', distance=15000.0)) == [
        [0],
        [0, 1],
    ]


def test_geodesic_nearest_handles_arrays_and_ties_deterministically() -> None:
    values = gm.points([0.0, 1.0, 50.0], [0.0, 0.0, 0.0], crs=4326)
    idx = gm.SpatialIndex(values)
    assert ids(
        idx.nearest(gm.points([0.4, 49.5], [0.0, 0.0], crs=4326), k=2, unit='meters')
    ) == [[0, 1], [2, 1]]
    tie = gm.SpatialIndex(gm.points([-1.0, 1.0, 10.0], [0.0, 0.0, 0.0], crs=4326))
    assert ids(tie.nearest(gm.Point(0.0, 0.0, crs=4326), k=2, unit='meters')) == [0, 1]


def test_insert_enforces_the_index_frame_and_never_reuses_handles() -> None:
    idx = gm.SpatialIndex([gm.Point(0, 0, crs=4326, epoch=2020.0)])
    with pytest.raises(gm.CRSMismatchError, match='share the index CRS'):
        idx.insert(gm.Point(0, 0, crs=3857, epoch=2020.0))
    with pytest.raises(ValueError, match='coordinate epoch'):
        idx.insert(gm.Point(1, 1, crs=4326, epoch=2021.0))
    assert idx.remove(0) is True
    assert idx.insert(gm.Point(2, 0, crs=4326, epoch=2020.0)) == 1


def test_self_join_finds_an_inserted_antimeridian_crossing_pair() -> None:
    """An inserted antimeridian-crossing geometry must index with its wrapped
    band, exactly like build-time rows: otherwise a later ``self_join`` misses
    the pair against a lower-id row whose narrow envelope cannot reach the
    crossing row's planar false-middle box (``j > i`` blocks the reverse emit).
    ``remove`` mirrors the same envelope so the entry round-trips.
    """
    box175 = gm.box(174.0, -5.0, 176.0, 5.0, crs=4326)
    idx = gm.SpatialIndex(gm.GeometryArray([box175]))
    crossing = gm.Polygon(
        [(170.0, -6.0), (-170.0, -6.0), (-170.0, 6.0), (170.0, 6.0), (170.0, -6.0)],
        crs=4326,
    )
    assert gm.intersects(box175, crossing)  # ground truth
    assert idx.insert(crossing) == 1
    assert pair_rows(idx.self_join(predicate='intersects')) == [(0, 1)]
    assert idx.remove(1) is True
    assert pair_rows(idx.self_join(predicate='intersects')) == []


def test_array_dwithin_query_widens_an_antimeridian_crossing_row() -> None:
    """The array/join dwithin row lane must widen a crossing query's planar
    bounds (like the scalar lane and the self-join), or candidates across the
    seam are missed.
    """
    idx = gm.SpatialIndex(gm.points([175.5], [0.0], crs=4326))
    crossing_line = gm.GeometryArray([
        gm.LineString([(170.0, 0.0), (-170.0, 0.0)], crs=4326)
    ])
    matches = idx.query(crossing_line, predicate='dwithin', distance=100000.0)
    assert match_rows(matches) == [[0]]


def test_self_join_intersects_agrees_with_brute_force_convex_and_concave() -> None:
    """The symmetric ``intersects`` self-join must agree with an O(n^2) brute
    force across mixed convex (overlapping boxes) and concave (L-shaped) rows.
    """
    import itertools

    geoms = [
        gm.box(i * 0.8, 0.0, i * 0.8 + 1.0, 1.0) for i in range(8)
    ]  # overlapping boxes
    geoms.append(  # concave L
        gm.Polygon([
            (0.0, 0.0),
            (3.0, 0.0),
            (3.0, 1.0),
            (1.0, 1.0),
            (1.0, 3.0),
            (0.0, 3.0),
        ])
    )
    idx = gm.SpatialIndex(gm.GeometryArray(geoms))
    got = set(pair_rows(idx.self_join(predicate='intersects')))
    brute = {
        (i, j)
        for i, j in itertools.combinations(range(len(geoms)), 2)
        if gm.intersects(geoms[i], geoms[j])
    }
    assert got == brute


def test_nearest_ties_returns_all_kth_distance_matches() -> None:
    """``ties=True`` recovers every row tying the k-th nearest distance
    (exact comparison) — shapely ``all_matches`` / geopandas ``return_all``
    parity, opt-in so the default contract keeps exactly ``k`` results.
    """
    rows = gm.GeometryArray([
        gm.Point(1, 0),
        gm.Point(-1, 0),
        gm.Point(0, 2),
        gm.Point(5, 5),
    ])
    idx = gm.SpatialIndex(rows)
    query = gm.Point(0, 0)
    assert len(idx.nearest(query)) == 1
    assert sorted(ids(idx.nearest(query, ties=True))) == [0, 1]
    assert sorted(ids(idx.nearest(query, k=3, ties=True))) == [0, 1, 2]
    nearest_ids, distances = idx.nearest(query, ties=True, return_distance=True)
    assert sorted(ids(nearest_ids)) == [0, 1]
    assert floats(distances) == pytest.approx([1.0, 1.0])
    matches = idx.nearest(gm.GeometryArray([query, gm.Point(5, 5)]), ties=True)
    assert sorted(ids(matches[0])) == [0, 1]
    assert ids(matches[1]) == [3]
    assert sorted(
        ids(gm.nearest([gm.Point(1, 0), gm.Point(-1, 0)], query, ties=True))
    ) == [0, 1]


def test_index_skips_empty_geometries() -> None:
    import gometry as gm

    idx = gm.SpatialIndex(
        gm.GeometryArray([
            gm.Point(1, 1, crs=4326),
            gm.from_wkt('POINT EMPTY', crs=4326),
            gm.Point(2, 2, crs=4326),
        ])
    )
    assert sorted(idx.query(gm.Point(2, 2, crs=4326), predicate='intersects')) == [2]
    assert sorted(idx.query(gm.box(0, 0, 3, 3, crs=4326), predicate='intersects')) == [
        0,
        2,
    ]
