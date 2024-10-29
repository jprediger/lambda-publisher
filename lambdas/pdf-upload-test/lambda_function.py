import json
try:
    import boto3
    import botocore
except:
    pass
import traceback

# TESTE SCRIPT PUBLISH

#import sys
#print(sys.path)
#sys.path = ['/home/z/sn-upload-pdf', '/usr/local/lib/python312.zip', '/usr/local/lib/python3.12', '/usr/local/lib/python3.12/lib-dynload']

import struct
from io import StringIO
import base64
import functools
from PyPDF2 import PdfReader
from io import BytesIO

import concurrent.futures
import threading

from sn_http import buildReturn
from sn_http import request
from sn_http import doPost
from sn_pdf_db import do_connect
from sn_pdf_db import do_exec


pr_pdf = 'Given this text: "<<<text>>>", you will return a json array containing chunks from this text. Each chunk will be one or more paragraphs from this text, in the order they appear in the text. Combine two paragraphs in one chunk if one is a continuation of the previous paragraph matter, or if their subjects are similar. Each element of the json array must contain one column "topic" with the topic of the chunk, and one column "text" with the chunk text. Do not skip any text, each paragraph must be in at least one chunk. Do not wrap the json in markers'

def load_pdf2(pdf_name):
    

    s3 = boto3.resource('s3')
    obj = s3.Object('test-pdf-docs-sn', pdf_name+'.pdf')
    bb = obj.get()['Body'].read()
    pdf = PdfReader(BytesIO(bb))


    number_of_pages = len(pdf.pages)

    cchunks = list()

    for i in range(number_of_pages):
        page = pdf.pages[i]

        text = page.extract_text()

       # text = text.replace('\n',' ')

        prompt = pr_pdf.replace('<<<text>>>',text)
        r = request(prompt)
        #print(json.loads(r))
        cchunks.append(json.loads(r))

    print(cchunks)
    last = None
    fn = list()
    for chunk in cchunks:
        pdone = False
        if last:
            first = chunk[0]['text']

            prompt = """
                Consider these two texts:
                text1: '"""+last+"""',
                text2: '"""+first+"""'.
                Return only a number in the range 0-1 stating how likely it is that the text2 is a continuation of the text1. Include only the number, nothing else"""
            print(prompt)
            r = request(prompt)
            p = float(r)

            if p >= 0.7:

                fn = fn[:-1]
                fn.append(lt+'\n'+last+'\n'+first)
                pdone = True

        b = 0
        if pdone:
            b = 1

        for i in range(b,len(chunk)):
            c = chunk[i]
            fn.append(c['topic']+'\n'+c['text'])


        last = chunk[len(chunk)-1]['text']
        lt = chunk[len(chunk)-1]['topic']



    #f = open('ctx','w')
    emb = list()
    embeds = list()
    for x in fn:
        #print('ctx -- '+str(x))
     #   x = 'NEXT PIECE: '+str(x)+'\n'
      #  f.write(x)
        emb.append(str(x)+'\n')
    #f.close()
    #a


    for s in emb:
        ret = get_embedding(s)
        ldata = {'text':s,'vec':ret}
        embeds.append(ldata)


    dump(embeds,pdf_name)


    return buildReturn(200,cchunks)

def load_pdf3(pdf_name, do_db=False):
    s3 = boto3.resource('s3')
    obj = s3.Object('test-pdf-docs-sn', pdf_name + '.pdf')
    bb = obj.get()['Body'].read()
    pdf = PdfReader(BytesIO(bb))
    number_of_pages = len(pdf.pages)

    cfg = botocore.config.Config(max_pool_connections=100)
    lambda_client = boto3.client('lambda',config=cfg)
    
    # Teste com locks
    lock = threading.Lock()
    
    # Função para chamar lambda com processamento de pagina em paralelo
    def invoke_process_page(page, pr_pdf):
        print("starting page processing")
        page_text = page.extract_text()
        response = lambda_client.invoke(
            FunctionName='pdf-chunk-processing',
            InvocationType='RequestResponse',
            Payload=json.dumps({'action': 'process_page', 'page_text': page_text, 'pr_pdf': pr_pdf})
        )
        result = json.loads(response['Payload'].read())
        print(result)
        processing = json.loads(result['body'])
        return processing
    
    # Função para chamar lambda com comparação de chunks
    def invoke_compare_chunks(last, first):
        response = lambda_client.invoke(
            FunctionName='pdf-chunk-processing',
            InvocationType='RequestResponse',
            Payload=json.dumps({'action': 'compare_chunks', 'last': last, 'first': first})
        )
        result = json.loads(response['Payload'].read())
        return float(result['body'])
        
    # Função para invocar outra Lambda para obter os embeddings
    def invoke_get_embedding(text):
        response = lambda_client.invoke(
            FunctionName='pdf-chunk-processing',  # Nome da função Lambda que gera embeddings
            InvocationType='RequestResponse',
            Payload=json.dumps({'action': 'get_embedding','text': text})
        )
        result = json.loads(response['Payload'].read())
        embedding = json.loads(result['body'])
        res = embedding['data'][0]['embedding']
        return res

    cchunks = []
    
    # leitura paginas
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        cchunks = list(executor.map(lambda i: invoke_process_page(pdf.pages[i], pr_pdf), range(number_of_pages)))
    print(cchunks)
    
    last = None
    fn = []

    # comparação chunks
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        for chunk in cchunks:
            
            if last:
                first = chunk[0]['text']
                try:
                    p = executor.submit(invoke_compare_chunks, last, first).result()
                    print(f"Probabilidade de continuação: {p}")
                    if p >= 0.7:
                        with lock:
                            fn[-1] = lt + '\n' + last + '\n' + first # Atualiza o último chunk em vez de removê-lo
                        chunk = chunk[1:]  # Remove o primeiro chunk, pois é uma continuação
                except Exception as e:
                    print(f"Erro ao comparar chunks: {e}")
                    
            with lock:
                fn.extend(f"{c['topic']}\n{c['text']}" for c in chunk)

            last, lt = chunk[-1]['text'], chunk[-1]['topic']
            print(f"Último texto e tópico atualizados: Last - {last}, Tópico - {lt}")


    # grava os chunks, pra debug
    s3 = boto3.resource('s3')
    obj = s3.Object('test-pdf-docs-sn', pdf_name+'.pdf.chunks')
    f = json.dumps(cchunks).encode('utf-')
    obj. put(Body=f)
    
    
    # embeddings
    #embeds = [{'text': f"{x}\n", 'vec': get_embedding(f"{x}\n")} for x in fn]
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        embeds = list(executor.map(lambda x: {'text': f"{x}\n", 'vec': invoke_get_embedding(f"{x}\n")}, fn))
    
    if embeds:
        dump(embeds, pdf_name)
        print(f"Dados coletados para salvar: {embeds}")
    else:
        print("Nenhum dado foi coletado para ser salvo.")
    """
    
    embed_lock = threading.Lock()
    embeds = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = []

        def process_embedding(x):
            try:
                result = {'text': f"{x}\n", 'vec': invoke_get_embedding(f"{x}\n")}
                with embed_lock:
                    embeds.append(result)
            except Exception as e:
                print(f"Erro ao processar embedding para '{x}': {e}")

        for x in fn:
            futures.append(executor.submit(process_embedding, x))

        concurrent.futures.wait(futures)

    if embeds:
        dump(embeds, pdf_name,do_db)
        print(f"Dados coletados para salvar: {embeds}")
    else:
        print("Nenhum dado foi coletado para ser salvo.")


    return buildReturn(200, cchunks)
    
def parse_paper(pdf):

    number_of_pages = len(pdf.pages)

    paper_text = []
    for i in range(number_of_pages):
        page = pdf.pages[i]
        page_text = []
        paragraphs = []

        global s
        s = StringIO()
        global lastfont
        global lasty
        lastfont = 0
        lasty = 0

        def visitor_body(text, cm, tm, fontDict, fontSize):
            x = tm[4]
            y = tm[5]
            #y = cm[5]
            global s
            global lastfont
            global lasty

            ltext = text.strip().replace(' ',' ').replace('\n','').encode('utf-8','ignore').decode("utf-8")
#            print('VISIT '+ltext)

            if len(ltext) != 0:

                #print(str(ltext)+' '+str(lasty-y)+' '+str(y)+' '+str(lasty)+' '+str(tm)+' '+str(cm))
                forceLine = False

                if ltext.strip().encode('utf-8')[0] == 226:
                    forceLine = True;
                if forceLine or (lasty-y) > 1.5*lastfont:
                    ss = s.getvalue()
                    if (len(ss) > 30):
                        paper_text.append(ss)

                    s = StringIO()



                s.write(ltext+' ')

                lastfont = fontSize
                lasty = y

        page.extract_text(visitor_text=visitor_body)

        ss = s.getvalue()
        if (len(ss) > 30):
            paper_text.append(ss)

    return paper_text

openai_embeddings_url = 'https://api.openai.com/v1/embeddings'
openai_key = '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'

def get_embedding(text):
    text = text.replace("\n", " ")
     #print(prompt)
    key = ''
    if key == '':
        key = openai_key

    js = {
        "input": text,
        "model": "text-embedding-3-large"
    }

    st,result = doPost(openai_embeddings_url,key,js)
    print(result)
    result = json.loads(result)

    res = result["data"][0]["embedding"]
    return res


def load_pdf(pdf_name):

    s3 = boto3.resource('s3')

    obj = s3.Object('test-pdf-docs-sn', pdf_name+'.pdf')
    bb = obj.get()['Body'].read()
    pdf = PdfReader(BytesIO(bb))
    paper_text = parse_paper(pdf)

    fn = list()
    i = 0
    while i < len(paper_text):
        if i < len(paper_text)-1:
            s = str(paper_text[i])
            s2 = str(paper_text[i+1])
            while True:
                prompt = 'Given the two following texts: "'+s+'" and "'+s2+'" return in a 0 to 1 probability range how likely it is that these two texts have the same context, or complement each other in terms of information. Provide only the calculated probability as a number in the response and nothing more .'

                #r = openai.ChatCompletion.create(model="gpt-3.5-turbo",temperature=0.4,messages=[{"role": "user", "content": prompt},])
                r = request(prompt)
                print('p1 '+s)
                print('p2 '+s2)
                print(r)
                p = r
                p = float(p)

                if (p >= 0.7):
                    i = i + 1
                    s = str(s) + ' '+str(s2)
                    print(s)
                    if i >= (len(paper_text)-1):
                        break
                    s2 = paper_text[i+1]
                    print(s2)
                else:
                    break
            fn.append(s)
        i = i + 1
    fn.append(paper_text[len(paper_text)-1])
    #f = open('ctx','w')
    emb = list()
    embeds = list()
    for x in fn:
        #print('ctx -- '+str(x))
        emb.append(str(x)+'\n')


    for s in emb:
        ret = get_embedding(s)
        ldata = {'text':s,'vec':ret}
        embeds.append(ldata)


    dump(embeds,pdf_name)

def dump(rt,pdf_name,do_db=False):

    if do_db:
        c = do_connect()
        name = pdf_name

        do_exec(c,"delete from v_data where name = '"+pdf_name+"'")
        c.commit()
        
        for i in rt:
            text = i['text']
            emb = 'array[['
            for d in i['vec']:
                emb = emb+str(d)+','

            emb = emb[:-1] + str(']]')
            sql = "insert into v_data (name,text,emb) values ('"+name+"','"+text+"',"+emb+")"
            #print(sql)
            do_exec(c,sql)
            
            #break

        c.commit()

        c.close()
        print("DB dump complete")


    s3 = boto3.resource('s3')
    obj = s3.Object('test-pdf-docs-sn', pdf_name+'.pdf.serialized')

    f = bytearray()

    f.extend(struct.pack('<L', len(rt)))
    for i in rt:
        st = bytes(i['text'],'utf-8')
        f.extend(struct.pack('<L', len(st)))
        f.extend(st)
        f.extend(struct.pack('<L', len(i['vec'])))
        for d in i['vec']:
            f.extend(struct.pack('<d', d))

    obj. put(Body=f)


def lambda_handler(event, context):
    if event['httpMethod'] == 'OPTIONS':
        return buildReturn(200,'')
    print(event)
    body = json.loads(event['body'])
    if not 'action' in body:
        return buildReturn(400,'missing action')
    if not 'pdf_nome' in body:
        return buildReturn(400,'missing parameters')

    action = body['action']
    key = body['pdf_nome']

    
    if action == 'upload':

        s3_client = boto3.client('s3')

        response = s3_client.generate_presigned_post(Bucket='test-pdf-docs-sn' , Key= key+'.pdf')
        #response = s3_client.generate_presigned_url('put_object', Params={'Bucket': 'test-pdf-docs-sn' , 'Key': key+'.pdf'}, ExpiresIn=3600)
        return buildReturn(200,response)
    elif action == 'test_db':
        c = do_connect()
        do_exec(c,"insert into v_data (name,text,emb) values ('snrh','Supernova Saúde',array[[-0.012457361,0.023027048]])")
        c.commit()
        c.close()
        return buildReturn(200,'')
    elif action == 'process':
        try:
            if 'okey' in body:
                print('okey '+body['okey'])
                dynamodb = boto3.client('dynamodb')
                dynamodb.put_item(TableName='ia-keys', Item={'nome':{'S':key},'key':{'S':body['okey']}})

            do_db = False
            if 'db' in body:
                do_db = True
            load_pdf3(key,do_db)
            return buildReturn(200,'')
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            return buildReturn(500,'')



