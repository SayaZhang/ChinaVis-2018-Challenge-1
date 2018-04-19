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
        # if date_subject_count_dict.get(date_sent) is None or len(date_subject_count_dict.get(date_sent)) < 10:
        #     date_subject_count_dict.setdefault(date_sent, {})
        #     date_subject_count_dict[date_sent].setdefault(subject, 0)
        #     date_subject_count_dict[date_sent][subject] += int(count)
        date_subject_count_dict.setdefault(date_sent, {})
        date_subject_count_dict[date_sent].setdefault(subject, 0)
        date_subject_count_dict[date_sent][subject] += int(count)

    total_subject_count = {}
    for date_sent in date_subject_count_dict.keys():
        subject_count = date_subject_count_dict.get(date_sent)
        for subject in subject_count.keys():
            total_subject_count.setdefault(subject, 0)
            total_subject_count[subject] += subject_count.get(subject)

    out_file = 'top_' + input_path
    try:
        out = open(out_file, 'w')
        try:
            for (subject, count) in sorted(total_subject_count.items(), key=lambda e: e[1], reverse=True):
                out.write('%s\t%d\n' % (subject, count))
        finally:
            out.close()
    except:
        print 'can not open ' + out_file


if __name__ == '__main__':
    merge_top10_subjects('subject_period_201101-201306.txt')
    merge_top10_subjects('subject_period_201307-201401.txt')
    merge_top10_subjects('subject_period_201402-201405.txt')
    merge_top10_subjects('subject_period_201406-201505.txt')
