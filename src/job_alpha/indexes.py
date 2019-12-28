from __future__ import division
import os,sys,inspect
import re
import pandas as pd
import operator

from .language import Language

class JobIndex(object):

    def weighted_job_ranking(self, ticker, jobs):
        tick = jobs[jobs['ticker'] == ticker]
        titles = [Language.get_title(t) for t in tick['title'].values]
        return self.get_job_rank(titles)

    def calc_weight(self, tokens, weight):
        ''' weight the position based on the relative tier weights '''
        default = 30
        scaler = 5
        if len(tokens) < 1:
            return default
        if len(tokens) == 1:
            if len(tokens[0]) > 1:
                # +/- scaler
                return weight[0] + (len(tokens[0]) * scaler) 
            return weight[0]
        if len(tokens) > 1:
            # avg
            return sum(weight)/len(weight)

    def get_job_rank(self, titles):
        ''' rank each title for an index weighting '''
        results = []
        # job tiers
        t1 = ['executive', 'chief', 'advisor']
        t2 = ['senior', 'superintendent', 'engineer', 'principal', 'manager']
        t3 = ['supervisor', 'analyst', 'intermediate']
        t4 = ['specialist', 'technician', 'coordinator' ]
        t5 = ['junior', 'administrator', 'clerk', 'assistant']

        detier = ['trainee', 'student', 'graduate']

        # weights
        t1_weight = 90
        t2_weight = 70
        t3_weight = 50
        t4_weight = 30
        t5_weight = 10
        weights = [t1_weight, t2_weight, t3_weight, t4_weight, t5_weight]

        for title in titles:
            #print('----', title, '----')
            tokens = []
            weight = []
            for i, tier in enumerate([t1, t2, t3, t4, t5]):
                match = r'\b(?:{})\b'.format('|'.join([t + 's?' for t in tier]))
                #print(match)
                token = re.findall(match, title)
                if not token:
                    continue
                tokens.append(token)
                weight.append(weights[i])
            rank = self.calc_weight(tokens, weight)/100
            print(title, rank)
            results.append((titles, rank))

        ranks.sort(key=operator.itemgetter(1), reverse=True)
        return results
