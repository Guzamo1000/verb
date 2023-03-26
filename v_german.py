import requests,os
from bs4 import BeautifulSoup
import json
import codecs
import pymongo
from concurrent.futures import ThreadPoolExecutor
import threading
class verb_german:
    def __init__(self,path):
        self.path=path
        self.client = pymongo.MongoClient("mongodb+srv://hauuu:258000@cluster0.sh77m4v.mongodb.net/?retryWrites=true&w=majority") 
        self.db=self.client['verb']
        self.collection=self.db['german']
    def get_verb(self,file_path):
        print(f"file path {file_path}")
        # file_path="/home/hauuu/Desktop/eup/fazzta/raw/aalen.html"
        with open(file_path,'r') as f:
            content=f.read()
        soup=BeautifulSoup(content,"html.parser")

        file={}

        key=soup.find("div",class_="headerDescription conj clearfix").find("h1").find("b").text

        file['key']=key
        word_wrap_row=soup.find_all("div", class_="word-wrap-row")

        if len(word_wrap_row)==0: 

            file['status']="false"

            return file
        
            
        verb_=[]
        body_middle=[]
        imperativ_prasens={}
        for wr in range(5):
            body_verb_={}
            if wr!=1 and wr!=4:
                verformen=word_wrap_row[wr].find("div",class_="word-wrap-title").find("h4").text
            wrap_three_col=word_wrap_row[wr].find_all("div", class_="wrap-three-col")
            body=[]
            for w in range(len(wrap_three_col)):
                wl=wrap_three_col[w]
                body_verbformen={}
                if w<2 and wr<4:
                    tense=wl.find("div",class_="blue-box-wrap").find("p").text
                value_verb=wl.find("ul").find_all("li")
                if len(value_verb)==0: continue
                conjugation=[]
                for v in value_verb:
                    body_conjugation={}
                    verb=v.find_all("i", class_=['particletxt','verbtxt',"hglhOver"])
                    verb=[v.text for v in verb]
                    verb="".join(verb)

                    personalpronomen=v.find("i",class_="graytxt").text  
                    body_conjugation['verb']=verb
                    body_conjugation['personalpronomen']=personalpronomen
                    conjugation.append(body_conjugation)
                if wr==4 and w==2:
                    li=wl.find_all("li")
                    body_conjugation={}
                    for l in li:
                        # print(f"l: {l}")
                        verb=l.find("i", class_=["verbtxt","hglhOver"]).text
                        personalpronomen=l.find("i",class_="graytxt").text
                        body_conjugation['verb']=verb
                        body_conjugation['personalpronomen']=personalpronomen
                    conjugation.append(body_conjugation)
                    verbformen=wl.find("div",class_='word-wrap-title').find("h4").text
                    imperativ_prasens['verbformen']=verbformen
                    imperativ_prasens['body']=conjugation
                    verb_.append(imperativ_prasens)
                body_verbformen['tense']=tense
                body_verbformen['conjugation']=conjugation
                body.append(body_verbformen)
            if wr==0: body_middle = body
            elif wr==1: 
                body= body_middle + body
            if wr>=1 and wr<4:
                body_verb_['verbformen']=verformen
                body_verb_['body']=body
                verb_.append(body_verb_)
        
        # word-wrap-row 5
        col=word_wrap_row[5].find_all("div",class_="wrap-three-col")
        body5={}
        conjugation2=[]
        # col 3
        conjugation3=[]
        body_conjugation3={}
        verbformen=col[2].find("h4").text
        tense=col[2].find("div", class_='blue-box-wrap alt-tense').find("p").text
        verb=col[2].find("i",class_="verbtxt").text
        body_conjugation3['tense']=tense
        body_conjugation3['verb']=verb
        body5['verbformen']=verbformen
        body5['body']=body_conjugation3
        verb_.append(body5)
        body5={}
        # col 1
        verbformen=word_wrap_row[5].find("div", class_="word-wrap-title two-col-left").find("h4").text
        body5['verbformen']=verbformen
        tense=col[0].find("div",class_="blue-box-wrap").find("p").text
        li=col[0].find_all("li")
        if len(li)!=0: 
            verb=col[0].find("i").text
        body_conjugation2={}
        body_conjugation2["tense"]=tense
        body_conjugation2['verb']=verb
        conjugation2.append(body_conjugation2)
        # col 2
        
        tense=col[1].find("div",class_="blue-box-wrap").find("p").text
        verb=col[1].find_all("i",class_=['particletxt','verbtxt',"hglhOver"])
        # print(f"verb {verb}")
        verb=[v.text for v in verb]
        verb="".join(verb)
        body_conjugation2['tense']=tense
        body_conjugation2['verb']=verb
        conjugation2.append(body_conjugation2)
        
        #word_wrap_row 6
        body_conjugation2={}
        body_conjugation2["tense"]=tense
        body_conjugation2['verb']=verb
        conjugation2.append(body_conjugation2)
        col=word_wrap_row[6].find_all("div",class_="wrap-three-col")
        conjugation4=[]
        for c in col:
            body_conjugation4={}
            tense=c.find("div",class_='blue-box-wrap alt-tense').find("p").text
            verb=col[1].find_all("i",class_=['particletxt','verbtxt',"hglhOver"])
            # print(f"verb {verb}")
            verb=[v.text for v in verb]
            verb="".join(verb)
            body_conjugation4['tense']=tense
            body_conjugation4['verb']=verb
            conjugation4.append(body_conjugation4)
        conjugation2=conjugation2+conjugation4
        body5['body']=conjugation2
        verb_.append(body5)

        # print(f"ver_ : {verb_}")
        # print(f"5: {conjugation2}")
        file['verb_body']=verb_
        file['status']=True
        self.collection.insert_one(file)
        return file
    def run(self):
        file_list=os.listdir(self.path)
        verb_in_all_file=[]
        thread=[]
        processed_files=0
        with ThreadPoolExecutor(max_workers=40) as executor:
            for file_name in file_list:
                processed_files += 1
                print(f"{processed_files/len(file_list)*100:.2f}%")
                # file_name=file_list[file]
                if file_name.endswith(".html"):
                    file_path=os.path.join(self.path,file_name)
                    verb=self.get_verb(file_path)
                    
                    executor.submit(self.get_verb,file_path)

                    verb_in_all_file.append(verb)
        
        print("complete ")
        # print(f"verb_in_all_file {verb_in_all_file}")
        # with open("data.json",'w',encoding="utf-8") as f:
            # json.dump(verb_in_all_file,f,ensure_ascii=False)




german=verb_german("raw")
german.run()
# print(german.get_verb("test_crawl/mißwirtschaftend.html"))