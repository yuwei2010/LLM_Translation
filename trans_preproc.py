import pandas as pd
import numpy as np
from datasurfer.lib_objects.pdf_object import PDFPagesObject
from datasurfer.datautils import xml_valid_df

#%%
def remove_title_line(df):
    count = 0
    for i, (text, num) in df[['text', 'rel_pagenum']].iterrows():  
        if num > count:
            count += 1       
        else:        
            yield([text, num])

#%%

def split_paragraph(text, threshold=0.8):
    
    lst_text = np.array([txt for txt in text.split('\n') if txt.strip()])
    larr = np.array([len(txt) for txt in lst_text])
    isnewline = larr < (larr.max() * threshold)
    
    out = ''
    for isnl, txt in zip(isnewline, lst_text):
        if isnl:
            out += txt
            yield out
            out = ''
        else:
            out += txt.rstrip('-')
            
    if len(out) > 0:      
        yield out
        
#%%
def yield_paragraphs(out0, threshold):

    for text, num in out0:
        for txt in split_paragraph(text, threshold=threshold):
            yield [txt, num]
            
#%%
def merge_paragraphs(out1):
    
    iterator = iter(out1)
    line0, num0 = next(iterator)
    while True:
        try:       
            line1, num1 = next(iterator)
        except StopIteration:
            yield line0, num0
            break
        if num1 > num0:
        
            if line0.strip()[-1] not in '.!?:;':
                line = line0 + ' ' + line1
                yield line, num0
                try:
                    line0, num0 = next(iterator)
                except StopIteration:
                    yield line0, num0
                    break
            else:
                yield line0, num0
                line0 = line1
                num0 = num1            
        else:
            yield line0, num0   
            line0 = line1
            num0 = num1

#%%
def extract_chapter_text(pdf_path, rng, threshold=0.8):
    
    pobj = PDFPagesObject(pdf_path, rng)
    df = pobj.dataframe
    df = df[df.text.str.strip().str.len()>0].reset_index(drop=True)
    df['rel_pagenum'] = df.page_num.astype(int) - df.page_num.astype(int).min()
    df = df[df.text.str.contains(r'[a-zA-Z]+', regex=True)].reset_index(drop=True)
    
    out = list(merge_paragraphs(yield_paragraphs(remove_title_line(df), threshold=threshold)))
    
    dfout = pd.DataFrame(out, columns=['Original', 'Page'])
    dfout['Translation'] =  None
    dfout['Review'] = 4 

    dfout = xml_valid_df(dfout[['Page', 'Original', 'Translation', 'Review']])
    dfout.Page = dfout.Page.astype('int') + df.page_num.min() + 1

    dfout = dfout[~dfout['Original'].str.isnumeric()]
    dfout = dfout[~(dfout['Original'].str.strip().str.len()<2)]
    dfout.reset_index(drop=True, inplace=True)
    return dfout           

#%%

if __name__ == '__main__':

    pass
