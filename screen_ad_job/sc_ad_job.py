import re # 正则表达式库
import pandas as pd
import collections # 词频统计库
import numpy as np # numpy数据处理库
import jieba # 结巴分词
import math
import itertools

ad_info= pd.read_csv('./广告相关信息/广告相关信息.csv')
employ_ad=ad_info.loc[ad_info['广告文案'].str.contains('招聘|岗位|待遇|薪资',na=False)]
employ_ad=employ_ad.drop_duplicates()
employ_ad.to_csv('./招聘广告.csv',index=False)
ad_random=ad_info[~ad_info['编号'].isin(employ_ad['编号'])].sample(3330)
ad_random.to_csv('./非招聘广告.csv',index=False)

filename = pd.read_excel('./招聘广告_true1.xls',encoding='UTF-8')
filename1 = pd.DataFrame()
filename1=filename['广告文案']
filename1.to_csv('./title.csv',index = False,header = False)

filename2 = pd.read_csv('./非招聘广告.csv',encoding='UTF-8')
filename3 = pd.DataFrame()
filename3=filename2['广告文案']
filename3.to_csv('./title_no.csv',index = False,header = False)

def stopwordslist():
    stopwords = [line.strip() for line in open('stopwords.txt', encoding='GB18030').readlines()]
    return stopwords

# 对句子进行中文分词
def seg_depart(sentence):
    # print("正在分词")
    #print(sentence)
    sentence_depart = jieba.cut(sentence.strip())
    # 创建一个停用词列表
    stopwords = stopwordslist()
    # 输出结果为outstr
    outstr = ''
    # 去停用词
    for word in sentence_depart:
        if word not in stopwords:
            if word != '\t':
                outstr += word
                outstr += " "
    return outstr


# 给出文档路径
infilename = './title_no.csv'
outfilename = './fen-search_no.csv'
# 将输出结果写入fen-search.csv中
outputs = open(outfilename, 'w', encoding='UTF-8')
with open (infilename,'r',encoding='UTF-8') as inputs:
    for line in inputs.readlines():
        #isspace()方法判断当该行是空行时，跳过该行
        if line.isspace():
            continue
        else:
            line_seg = seg_depart(line)
            outputs.write(line_seg + '\n')
    inputs.close()
    outputs.close()


# 文本预处理
pattern = re.compile(u'\t|\n|\.|-|:|;|\)|\(|\?|"|？|，'
                     u'|,|\+|/|（|“|【|】|\[|\]|）|<|>|{|}|'
                     u'《|》|-|_|;|；|。|：|#|！|、|!|”') # 定义正则表达式匹配模式
# 给出文档路径
filename = './fen-search_no.csv'
inputs = open(filename, 'r', encoding='UTF-8')
#统计词频
word_tf_dic = {}
for line in inputs:
    if line.isspace():
        continue
    else:
        line = re.sub(pattern,'', line)# 将符合模式的字符去除
        s = line.split()
        # s.sort()
        for xs in s:
            #print(xs)
            if xs not in word_tf_dic:
                word_tf_dic[xs] = 1
            else:
                word_tf_dic[xs] += 1
z = word_tf_dic
a = list(z.keys())
# 字典中的value转换为列表
b = list(z.values())
frequence_array=np.array(a)[:,np.newaxis]
amplitude_array=np.array(b)[:,np.newaxis]
concatenate_array=np.concatenate((frequence_array,amplitude_array),axis=1)
data=pd.DataFrame(concatenate_array,columns=["搜索内容","次数"])
data['次数']=data['次数'].astype(int)
data1=data.sort_values(by='次数', ascending=False)
data1.to_csv('nozhaop_ad.csv',index=False)


zhaopdf =  pd.read_csv('./zhaop_ad.csv')
zhaopdf['次数'].sum()
#3133
nozpdf =  pd.read_csv('nozhaop_ad.csv')
nozpdf['次数'].sum()
#34192
count = 0
word_tf_dic = {}
#获取招聘文档的词的mi值
for index, row  in nozpdf.iterrows():
    lquery = row['搜索内容']
    l1 = row['次数']
    l0 = 34192-l1
    a = zhaopdf[(zhaopdf['搜索内容']==lquery)]
    if a.empty==1:
        r1 = 0
        r0 = 3133
        sumlr = 3133 + 34192
        mi = (l1/sumlr*math.log2((sumlr*l1)/((l1+r1)*(l1+l0)))) +(r1+1/sumlr*math.log2((sumlr*r1+1)/((l1+r1)*(r1+r0)))) + (l0/sumlr*math.log2((sumlr*l0)/((l0+r0)*(l1+l0))))+ (r0/sumlr*math.log2((sumlr*r0)/((l0+r0)*(r1+r0))))
    else:
        r1 = a.iloc[0].iat[1]
        r0 = 34192 - r1
        sumlr = 3133 + 34192
        mi = (l1/sumlr*math.log2((sumlr*l1)/((l1+r1)*(l1+l0)))) +(r1/sumlr*math.log2((sumlr*r1)/((l1+r1)*(r1+r0)))) + (l0/sumlr*math.log2((sumlr*l0)/((l0+r0)*(l1+l0))))+ (r0/sumlr*math.log2((sumlr*r0)/((l0+r0)*(r1+r0))))
    word_tf_dic[lquery] = mi
z = word_tf_dic
a = list(z.keys())
# 字典中的value转换为列表
b = list(z.values())
frequence_array=np.array(a)[:,np.newaxis]
amplitude_array=np.array(b)[:,np.newaxis]
concatenate_array=np.concatenate((frequence_array,amplitude_array),axis=1)
data=pd.DataFrame(concatenate_array,columns=["搜索内容","次数"])
data['次数']=data['次数'].astype(float)
data1=data.sort_values(by='次数', ascending=False)
data1.to_csv('no_mi.csv',index=False)

key_word1 = ['招聘','薪资','兼职']
key_word2 = ['招聘','工作','薪资','兼职','待遇','一金','学历','经理','月薪','高薪','急招','岗位',
             '优厚','培训','五险','包吃','客服','城市','保安','快递','工资','双休','日结','底薪','年薪']
c=ad_info[ad_info['广告文案'].str.contains('|'.join(key_word1),na=False)]
key_word3 = list(itertools.combinations(key_word2, 2))
b=pd.DataFrame(columns=c.columns)
for i in key_word3:
    a = ad_info[ad_info['广告文案'].str.contains(i[0],na=False)&
                ad_info['广告文案'].str.contains(i[1],na=False)]
    b=b.append(a)
c=c.append(b)
c=c.drop_duplicates()
c.to_csv('./招聘广告_final.csv',index=False)
