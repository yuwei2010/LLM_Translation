import pandas as pd
import numpy as np
import datasurfer as ds
import time
import warnings
from pathlib import Path
from datasurfer.lib_llm.llm_agents import LLMAAgent
from datasurfer.lib_objects.pdf_object import PDFPagesObject
from datasurfer.lib_objects.xlsx_object import XLSXObject
#%%


#%%
async def multiagent_translate(obj, retry=10, memory_length=100, timeout=300, version='R1V1', nsample=2, strmap=''):
    
    async def transreview_text(original):

        count = 0 
        while count < retry:
            try:
                translation = await Linda.told(pattern_Linda.format(original=original), use_cache=False, memory_length=memory_length, timeout=timeout, silent=True)

                if 'Instruction' not in translation:
                    break
            except Exception as e:
                warnings.warn(f'Error: {e}')
                
            count += 1
        else:
            raise Exception(f'Failed to translate "{original}" after {retry} retries.')
                
        reviewed = await Robin.told(pattern_Robin.format(original=original, translation=translation), use_cache=False, memory_length=memory_length*2, timeout=timeout, silent=True)
        return [original, reviewed] 
    
       
    async def start_translation(df, nrows):
        
        out = []
        buffer = []
        
        for idx, (page, original, trans, review) in df.iterrows():
            start = time.time()
            if review != 0:
                print(f'Processing "{stem}" {idx+1}/{nrows} ({(idx)/nrows*100:0.2f}%)...\n')
                #Linda.print_message(f'{original}', 80, role='User')
            
            if review != 2:
                if len(buffer):
                    merged = ' '.join(buffer)
                    buffer = []
                    trtxt = await transreview_text(merged)
                    out.append([page, *trtxt])     
                                
            if review == 4:
                trtxt = await transreview_text(original)
                out.append([page, *trtxt])  
                
            elif review == 2:
                buffer.append(original)
                
            elif review == 1:
                continue
            
            elif review == 3:
                txts = original.split('@')
                
                for txt in txts: 
                    trtxt = await transreview_text(txt)
                    out.append([page, *trtxt])
            
            elif review == 0:
                Linda.append_history(pattern_Linda.format(original=original), role='User')
                Linda.append_history(trans)
                Robin.append_history(pattern_Robin.format(original=original, translation=trans), role='User')
                Robin.append_history(trans)
                out.append([page, original, trans])
            else:
                raise ValueError(f'Invalid review value: {review}')

            if review != 0:
                duration = time.time() - start
                tremain = int((nrows-idx-1)*duration)
                print(f'"{stem}" completed in {duration:0.2f}s, remain {tremain//3600:0.0f}h{tremain%3600//60:0.0f}m{tremain%60}s\n')
            
        return out
    
    Linda = LLMAAgent('Linda', 'You are a Chinese linguist, you translate German to Chinese. ' + strmap+'. ')
    Robin = LLMAAgent('Robin', 'You are a Chinese linguist, you also know German. ' + strmap + '. ')

    
    pattern_Linda = 'Translating "{original}" to Chinese, return only the translation, do not include any other words.'
    pattern_Robin = '根据德语原文\n"{original}"，\n将以下中文翻译改进到语义通顺, 修改其中的错误并去除不必要的句子：\n"{translation}"\n只返回修改过的不加引号的句子.'
        
    df_original = obj.df
    root = obj.path.parent.absolute()
    if 'Original' in obj.path.stem:
        stem = obj.path.stem.replace('Original', 'Translation') 
    elif 'Review' in obj.path.stem:
        stem = '_'.join(obj.path.stem.replace('Review', 'Translation').split('_')[:-1])
    else:
        raise ValueError('Invalid file name.')
    
    fdst1 = root / f'{stem}_{version}.xlsx'
    fdst2 = root / f'{stem}_{version}.docx'
    fbak  = root / f'{stem}_{version}.csv'   
     
    dfbak = pd.DataFrame() if not fbak.is_file() else pd.read_csv(fbak)


    while 1:
        
        idx_start = dfbak.index.max() + 1 if not dfbak.empty else 0
        
        if idx_start >= len(df_original):
            break
        
        df_working = dfbak.copy()
        df_working = pd.concat([df_working, df_original.loc[idx_start:min(idx_start+nsample, len(df_original)-1)]])
        df_working.fillna('', inplace=True)
        
        out = await start_translation(df_working, len(df_original))
        
        pages, raw_text, translation = zip(*out)
        dfout = pd.DataFrame({'Page': pages, 'Original': raw_text, 'Translation': translation, 'Review': 0})
        dfout.Translation = dfout.Translation.str.replace('Let me know if you have more text to translate!', '')
        print(f'Saving "{stem}"...\n')
        dfbak = dfout #pd.concat([dfbak, dfout])
        dfbak.to_csv(fbak, index=False)
    
    if not fdst1.is_file(): 
        dfbak.to_excel(fdst1, index=False)
    if not fdst2.is_file():
        obj = ds.DOCXObject(dfbak[['Original', 'Translation']], name='Translation')
        obj.save_df(fdst2)  
