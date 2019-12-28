import spacy

class Language(object):
    def __init__(self, *args):
        self.nlp = spacy.load('en_core_web_sm')

    def distill(self, document):
        self.doc = self.nlp(document)
        # extract object based noun chunks
        objs = [chunk.text for chunk in self.doc.noun_chunks if chunk.root.dep_ == 'pobj']
        # conjunctions with a root which is an object
        roots = [chunk.root.text for chunk in self.doc.noun_chunks if chunk.root.dep_ == 'pobj']
        conj = [chunk for chunk in self.doc.noun_chunks if chunk.root.dep_ == 'conj']
        conj_add = ['%s %s' % (chunk.root.head.text, chunk.text) for chunk in conj if chunk.root.head.text in roots]

        return ', '.join(objs + conj_add)

    def get_title(job_title):
        ''' clean the job title'''
        if job_title is None:
            return '' 
        job_title = job_title.replace('&', 'and')
        return job_title.replace(' - ', '|').split('|')[0].strip().lower()
        