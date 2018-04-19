__author__ = 'liyu-safe'

import re


def merge_top10_subjects(input_path):
    date_subject_count_dict = {}
    for line in open(input_path):
        if line.strip() == "":
            continue
        date_subject_count = line.strip().split('\t')
        date_sent, count = date_subject_count[0], date_subject_count[-1]
        if date_sent.strip() == "":
            continue
        subject = line.strip('\n').lstrip(date_sent).rstrip(count).strip('\t')
        date_subject_count_dict.setdefault(date_sent, {})
        date_subject_count_dict[date_sent].setdefault(subject, 0)
        date_subject_count_dict[date_sent][subject] += int(count)

    total_subject_count = {}
    for date_sent in date_subject_count_dict.keys():
        subject_count = date_subject_count_dict.get(date_sent)
        for subject in subject_count.keys():
            total_subject_count.setdefault(subject, 0)
            total_subject_count[subject] += subject_count.get(subject)

    return total_subject_count


def new_added_business(subject_count1, subject_count2,period_name):
    subject_period1 = set(subject_count1.keys())
    subject_period2 = set(subject_count2.keys())

    new_add_subjects_set = subject_period2.difference(subject_period1)
    new_add_subjects_dict = {}
    for subject in new_add_subjects_set:
        new_add_subjects_dict[subject] = subject_count2.get(subject)
    top_count = 0

    out = open('new_add_' + period_name, 'w')
    for (subject, count) in sorted(new_add_subjects_dict.items(), key=lambda e: e[1], reverse=True):
        top_count += 1
        # if top_count > 10:
        #    break
        out.write('%s\t%d\n' % (subject, count))
        # print subject, '\t', count
    out.close()
    print '*' * 100


if __name__ == '__main__':
    total_subject_count1 = merge_top10_subjects('subject_period_201101-201306.txt')
    total_subject_count2 = merge_top10_subjects('subject_period_201307-201401.txt')
    total_subject_count3 = merge_top10_subjects('subject_period_201402-201405.txt')
    total_subject_count4 = merge_top10_subjects('subject_period_201406-201505.txt')

    new_added_business(total_subject_count1, total_subject_count2,'subject_period_201307-201401.txt')
    new_added_business(total_subject_count2, total_subject_count3,'subject_period_201402-201405.txt')
    new_added_business(total_subject_count3, total_subject_count4,'subject_period_201406-201505.txt')
