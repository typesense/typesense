#include <gtest/gtest.h>
#include <or_iterator.h>
#include <posting_list.h>
#include <posting.h>
#include <filter_result_iterator.h>
#include "logger.h"

TEST(OrIteratorTest, IntersectTwoListsWith3SubLists) {
    std::vector<uint32_t> offsets = {0, 1, 3};

    std::vector<std::vector<uint32_t>> plists = {
            {0, 2, 3, 20},
            {1, 3, 5, 10, 20},
            {2, 3, 6, 7, 20}
    };

    std::vector<posting_list_t*> postings1(plists.size());

    for(size_t i = 0; i < plists.size(); i++) {
        postings1[i] = new posting_list_t(2);
        for(auto n: plists[i]) {
            postings1[i]->upsert(n, offsets);
        }
    }

    std::vector<std::vector<uint32_t>> ulists = {
            {0, 1, 5, 20},
            {1, 2, 7, 11, 15},
            {3, 5, 10, 11, 12}
    };

    std::vector<posting_list_t*> postings2(ulists.size());

    for(size_t i = 0; i < ulists.size(); i++) {
        postings2[i] = new posting_list_t(2);
        for(auto n: ulists[i]) {
            postings2[i]->upsert(n, offsets);
        }
    }

    std::vector<posting_list_t::iterator_t> pits1;
    for(auto& posting_list: postings1) {
        pits1.push_back(posting_list->new_iterator());
    }

    std::vector<posting_list_t::iterator_t> pits2;
    for(auto& posting_list: postings2) {
        pits2.push_back(posting_list->new_iterator());
    }

    or_iterator_t it1(pits1);
    or_iterator_t it2(pits2);

    std::vector<or_iterator_t> or_its;
    or_its.push_back(std::move(it1));
    or_its.push_back(std::move(it2));

    result_iter_state_t istate;

    std::vector<uint32_t> results;

    or_iterator_t::intersect(or_its, istate,
                             [&results](const single_filter_result_t& filter_result, std::vector<or_iterator_t>& its) {
        results.push_back(filter_result.seq_id);
    });


    ASSERT_EQ(8, results.size());

    std::vector<uint32_t> expected_results = {0, 1, 2, 3, 5, 7, 10, 20};
    for(size_t i = 0; i < expected_results.size(); i++) {
        ASSERT_EQ(expected_results[i], results[i]);
    }

    for(auto p: postings1) {
        delete p;
    }

    for(auto p: postings2) {
        delete p;
    }
}

TEST(OrIteratorTest, IntersectTwoListsWith4SubLists) {
    std::vector<uint32_t> offsets = {0, 1, 3};

    std::vector<std::vector<uint32_t>> plists = {
        {817, 2099, 2982, 3199, 5456, 6414, 8178, 8284, 8561, 10345, 13662, 14021, 15292},
        {9514},
        {5758, 13357}
    };

    std::vector<posting_list_t*> postings1(plists.size());

    for(size_t i = 0; i < plists.size(); i++) {
        postings1[i] = new posting_list_t(2);
        for(auto n: plists[i]) {
            postings1[i]->upsert(n, offsets);
        }
    }

    std::vector<std::vector<uint32_t>> ulists = {
        {15156},
        {242, 403, 431, 449, 469, 470, 471, 474, 476, 522, 538, 616, 684, 690, 789, 797, 841, 961, 970, 981, 1012, 1016, 1073, 1106, 1115, 1153, 1256, 1282, 1291, 1306, 1313, 1317, 1454, 1530, 1555, 1558, 1583, 1594, 1596, 1650, 1652, 1669, 1686, 1718, 1805, 1809, 1811, 1816, 1840, 1854, 1879, 1887, 1939, 1983, 2041, 2049, 2091, 2138, 2152, 2220, 2350, 2409, 2459, 2491, 2507, 2545, 2687, 2740, 2754, 2789, 2804, 2907, 2933, 2935, 2964, 2970, 3024, 3084, 3126, 3149, 3177, 3199, 3227, 3271, 3300, 3314, 3403, 3547, 3575, 3587, 3601, 3692, 3804, 3833, 3834, 3928, 4019, 4022, 4045, 4086, 4135, 4145, 4232, 4444, 4451, 4460, 4467, 4578, 4588, 4632, 4709, 4721, 4757, 4777, 4833, 4880, 4927, 4996, 5117, 5133, 5156, 5158, 5288, 5311, 5361, 5558, 5649, 5654, 5658, 5666, 5794, 5818, 5829, 5852, 5857, 5859, 5893, 5909, 5959, 5970, 5983, 5986, 6009, 6016, 6020, 6189, 6192, 6202, 6308, 6326, 6365, 6402, 6414, 6416, 6433, 6448, 6454, 6460, 6583, 6589, 6702, 7006, 7010, 7273, 7335, 7340, 7677, 7678, 7722, 7775, 7807, 7861, 7903, 7950, 7975, 8123, 8201, 8288, 8359, 8373, 8392, 8497, 8502, 8566, 8613, 8635, 8720, 8827, 8847, 8873, 9079, 9374, 9394, 9404, 9486, 9587, 9796, 9859, 9958, 10054, 10101, 10105, 10120, 10135, 10180, 10234, 10246, 10299, 10400, 10777, 11213, 11361, 11776, 11888, 12054, 12133, 12506, 12957, 12959, 12985, 13046, 13054, 13189, 13299, 13316, 13324, 13377, 13657, 13734, 14563, 14651, 14666, 14681, 14688, 14700, 14729, 14849, 14983, 14985, 15003, 15046, 15049, 15052, 15056, 15077, 15156, 15249, 15558, 15583, 15725, 15761, 15770, 15810, 16278, 16588, 17123, 17223,},
        {4, 235, 257, 261, 379, 394, 403, 449, 469, 621, 750, 758, 789, 790, 806, 820, 889, 910, 912, 921, 961, 992, 1000, 1005, 1012, 1036, 1153, 1155, 1176, 1394, 1407, 1412, 1450, 1454, 1475, 1486, 1594, 1633, 1650, 1654, 1669, 1675, 1686, 1766, 1871, 1879, 1939, 1983, 2023, 2056, 2197, 2226, 2255, 2332, 2459, 2491, 2507, 2513, 2526, 2538, 2545, 2546, 2567, 2591, 2592, 2749, 2825, 2834, 2843, 2849, 2920, 3013, 3024, 3061, 3062, 3183, 3219, 3319, 3503, 3657, 3667, 3692, 3728, 3740, 3751, 3804, 3807, 3860, 4022, 4112, 4120, 4123, 4135, 4262, 4343, 4375, 4388, 4444, 4467, 4588, 4762, 4829, 4927, 5107, 5109, 5117, 5241, 5288, 5411, 5558, 5654, 5675, 5710, 5744, 5760, 5778, 5781, 5823, 5893, 5974, 5986, 6000, 6009, 6012, 6016, 6067, 6114, 6192, 6222, 6253, 6259, 6287, 6308, 6337, 6338, 6349, 6384, 6387, 6416, 6433, 6442, 6454, 6476, 6576, 6589, 6619, 6719, 6727, 6875, 7084, 7221, 7335, 7340, 7355, 7619, 7670, 7775, 7781, 7861, 7961, 8000, 8017, 8191, 8268, 8363, 8412, 8484, 8737, 8833, 8872, 9121, 9125, 9311, 9322, 9359, 9413, 9491, 9532, 9694, 9735, 9895, 9911, 9958, 10105, 10120, 10180, 10299, 10302, 10318, 10327, 10372, 10375, 10378, 10391, 10394, 10400, 10458, 10487, 10497, 10556, 10564, 10569, 10631, 10657, 10662, 10777, 10781, 10827, 10872, 10873, 10923, 10961, 10975, 11043, 11224, 11702, 11776, 12025, 12149, 12318, 12414, 12565, 12734, 12854, 12945, 12971, 12977, 12997, 13008, 13032, 13054, 13064, 13103, 13143, 13170, 13205, 13209, 13220, 13224, 13255, 13299, 13348, 13357, 13377, 13381, 13385, 13516, 13537, 13588, 13626, 13631, 13643, 13669, 13700, 13752, 13788, 13813, 13817, 13914, 13935, 13974, 13999, 14111, 14236, 14544, 14549, 14627, 14688, 14712, 14985, 15012, 15137, 15148, 15155, 15297, 15302, 15386, 15388, 15416, 15418, 15576, 15583, 15584, 15608, 15636, 15679, 15685, 15686, 15690, 15693, 15742, 15753, 15756, 15762, 15783, 15805, 15810, 15819, 15906, 15910, 16093, 16232, 16278, 16479, 17027, 17123, 17223,}
    };

    // both lists contain common IDs: 3199, 6414, 13357

    std::vector<posting_list_t*> postings2(ulists.size());

    for(size_t i = 0; i < ulists.size(); i++) {
        postings2[i] = new posting_list_t(2);
        for(auto n: ulists[i]) {
            postings2[i]->upsert(n, offsets);
        }
    }

    std::vector<posting_list_t::iterator_t> pits1;
    for(auto& posting_list: postings1) {
        pits1.push_back(posting_list->new_iterator());
    }

    std::vector<posting_list_t::iterator_t> pits2;
    for(auto& posting_list: postings2) {
        pits2.push_back(posting_list->new_iterator());
    }

    or_iterator_t it1(pits1);
    or_iterator_t it2(pits2);

    std::vector<or_iterator_t> or_its;
    or_its.push_back(std::move(it1));
    or_its.push_back(std::move(it2));

    result_iter_state_t istate;

    std::vector<uint32_t> results;

    or_iterator_t::intersect(or_its, istate,
                             [&results](const single_filter_result_t& filter_result, std::vector<or_iterator_t>& its) {
        results.push_back(filter_result.seq_id);
    });

    std::vector<uint32_t> expected_results = {3199, 6414, 13357};

    ASSERT_EQ(expected_results.size(), results.size());

    for(size_t i = 0; i < expected_results.size(); i++) {
        ASSERT_EQ(expected_results[i], results[i]);
    }

    for(auto p: postings1) {
        delete p;
    }

    for(auto p: postings2) {
        delete p;
    }
}

TEST(OrIteratorTest, IntersectAndFilterThreeIts) {
    std::vector<uint32_t> offsets = {0, 1, 3};

    std::vector<std::vector<uint32_t>> id_list = {
        {4207, 29159, 47182, 47250, 47337, 48518, 99820,},
        {62, 330, 367, 4124, 4207, 4242, 4418, 28740, 29099, 29159, 29284, 40795, 43556, 46779, 47182, 47250, 47322, 48494, 48518, 48633, 98813, 98821, 99069, 99368, 99533, 99670, 99820, 99888, 99973,},
        {723, 1504, 29038, 29164, 29390, 30890, 34743, 35067, 36466, 40268, 40965, 42161, 43425, 45188, 47326, 47443, 49319, 53043, 58436, 58774, 61123, 70973, 71393, 81575, 82323, 88301, 88502, 88594, 88690, 88951, 90662, 91016, 91915, 92069, 92844, 99820,}
    };

    posting_list_t* p1 = new posting_list_t(256);
    posting_list_t* p2 = new posting_list_t(256);
    posting_list_t* p3 = new posting_list_t(256);

    for(auto id: id_list[0]) {
        p1->upsert(id, offsets);
    }

    for(auto id: id_list[1]) {
        p2->upsert(id, offsets);
    }

    for(auto id: id_list[2]) {
        p3->upsert(id, offsets);
    }

    std::vector<posting_list_t::iterator_t> pits1;
    std::vector<posting_list_t::iterator_t> pits2;
    std::vector<posting_list_t::iterator_t> pits3;
    pits1.push_back(p1->new_iterator());
    pits2.push_back(p2->new_iterator());
    pits3.push_back(p3->new_iterator());

    or_iterator_t it1(pits1);
    or_iterator_t it2(pits2);
    or_iterator_t it3(pits3);

    std::vector<or_iterator_t> or_its;
    or_its.push_back(std::move(it1));
    or_its.push_back(std::move(it2));
    or_its.push_back(std::move(it3));

    std::vector<uint32_t> filter_ids = {44424, 44425, 44447, 99820, 99834, 99854, 99859, 99963};
    result_iter_state_t istate(nullptr, 0, &filter_ids[0], filter_ids.size());

    std::vector<uint32_t> results;
    or_iterator_t::intersect(or_its, istate,
                             [&results](const single_filter_result_t& filter_result, std::vector<or_iterator_t>& its) {
        results.push_back(filter_result.seq_id);
    });

    ASSERT_EQ(1, results.size());

    delete p1;
    delete p2;
    delete p3;
}

TEST(OrIteratorTest, IntersectAndFilterTwoIts) {
    std::vector<uint32_t> offsets = {0, 1, 3};

    std::vector<std::vector<uint32_t>> id_list = {
            {4207, 29159, 47182, 47250, 47337, 48518, 99820,},
            {62, 330, 367, 4124, 4207, 4242, 4418, 28740, 29099, 29159, 29284, 40795, 43556, 46779, 47182, 47250, 47322, 48494, 48518, 48633, 98813, 98821, 99069, 99368, 99533, 99670, 99820, 99888, 99973,},
            {723, 1504, 29038, 29164, 29390, 30890, 34743, 35067, 36466, 40268, 40965, 42161, 43425, 45188, 47326, 47443, 49319, 53043, 58436, 58774, 61123, 70973, 71393, 81575, 82323, 88301, 88502, 88594, 88690, 88951, 90662, 91016, 91915, 92069, 92844, 99820,}
    };

    posting_list_t* p1 = new posting_list_t(256);
    posting_list_t* p2 = new posting_list_t(256);

    for(auto id: id_list[0]) {
        p1->upsert(id, offsets);
    }

    for(auto id: id_list[1]) {
        p2->upsert(id, offsets);
    }

    std::vector<posting_list_t::iterator_t> pits1;
    std::vector<posting_list_t::iterator_t> pits2;
    pits1.push_back(p1->new_iterator());
    pits2.push_back(p2->new_iterator());

    or_iterator_t it1(pits1);
    or_iterator_t it2(pits2);

    std::vector<or_iterator_t> or_its;
    or_its.push_back(std::move(it1));
    or_its.push_back(std::move(it2));

    std::vector<uint32_t> filter_ids = {44424, 44425, 44447, 99820, 99834, 99854, 99859, 99963};
    result_iter_state_t istate(nullptr, 0, &filter_ids[0], filter_ids.size());

    std::vector<uint32_t> results;
    or_iterator_t::intersect(or_its, istate,
                             [&results](const single_filter_result_t& filter_result, std::vector<or_iterator_t>& its) {
        results.push_back(filter_result.seq_id);
    });

    ASSERT_EQ(1, results.size());

    delete p1;
    delete p2;
}

TEST(OrIteratorTest, ContainsAtLeastOne) {
    std::vector<uint32_t> ids = {1, 3, 5};

    std::vector<or_iterator_t> or_iterators;
    std::vector<posting_list_t*> expanded_plists;

    posting_list_t p_list1(2);
    for (const auto &id: ids) {
        p_list1.upsert(id, {1, 2, 3});
    }
    void* raw_pointer = &p_list1;

    posting_t::get_or_iterator(raw_pointer, or_iterators, expanded_plists);
    ASSERT_EQ(1, or_iterators.size());

    posting_list_t p_list2(2);
    ids = {2, 4};
    for (const auto &id: ids) {
        p_list2.upsert(id, {1, 2, 3});
    }
    raw_pointer = &p_list2;

    posting_t::get_or_iterator(raw_pointer, or_iterators, expanded_plists);
    ASSERT_EQ(2, or_iterators.size());

    auto found = or_iterator_t::contains_atleast_one(or_iterators,
                                                     result_iter_state_t(nullptr, 0, nullptr));
    ASSERT_FALSE(found);

    or_iterators.clear();

    posting_list_t p_list3(2);
    ids = {1, 2, 4, 5};
    for (const auto &id: ids) {
        p_list3.upsert(id, {1, 2, 3});
    }

    raw_pointer = &p_list1;
    posting_t::get_or_iterator(raw_pointer, or_iterators, expanded_plists);
    raw_pointer = &p_list3;
    posting_t::get_or_iterator(raw_pointer, or_iterators, expanded_plists);
    ASSERT_EQ(2, or_iterators.size());

    found = or_iterator_t::contains_atleast_one(or_iterators,
                                                result_iter_state_t(nullptr, 0, nullptr));
    ASSERT_TRUE(found);
    ASSERT_EQ(1, or_iterators.front().id()); // Match found on id 1

    or_iterators.clear();

    raw_pointer = &p_list1;
    posting_t::get_or_iterator(raw_pointer, or_iterators, expanded_plists);
    raw_pointer = &p_list3;
    posting_t::get_or_iterator(raw_pointer, or_iterators, expanded_plists);
    ASSERT_EQ(2, or_iterators.size());

    auto filter_ids = new uint32_t[1]{5};
    auto filter_iterator = new filter_result_iterator_t(filter_ids, 1);
    found = or_iterator_t::contains_atleast_one(or_iterators,
                                                result_iter_state_t(nullptr, 0, filter_iterator));
    ASSERT_TRUE(found);
    ASSERT_EQ(5, or_iterators.front().id()); // Match found on id 5

    delete filter_iterator;
}
