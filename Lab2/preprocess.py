import jieba
import re
import os

from constants import DAT_DOC, DAT_ENCODING, DOC_FOLDER

# 处理.dat文件，将其中每个文档存入一个.txt文件
def preprocess_document(input_file, output_dir):
    with open(input_file, 'r', encoding=DAT_ENCODING) as file:
        data = file.read()

        # 提取.dat文档数据
        doc_pattern = re.compile(r'<doc>.*?</doc>', re.DOTALL)
        docs = doc_pattern.findall(data)

        # 解析其中的每个小文档
        for doc in docs:
            docno_match = re.search(r'<docno>(.*?)</docno>', doc)
            title_match = re.search(r'<contenttitle>(.*?)</contenttitle>', doc)
            content_match = re.search(r'<content>(.*?)</content>', doc)

            if docno_match and title_match and content_match:
                docno = docno_match.group(1)
                title = title_match.group(1).replace(" ", "")
                content = content_match.group(1).replace(" ", "")

                # 转换为UTF-8编码
                title = title.encode('utf-8').decode('utf-8')
                content = content.encode('utf-8').decode('utf-8')
                
                processed_data = f'{title}\n{content}\n'

                # 将处理后的数据写入.txt文件
                with open(f'{output_dir}/{docno}.txt', 'w', encoding='utf-8') as output_file:
                    output_file.write(processed_data)

# 计算提取后的.txt文件数量
def count_file(folder_path):
    file_count = 0
    for root, dirs, files in os.walk(folder_path):
        file_count += len(files)
    return file_count

# 分词处理
def tokenize(text):
    words = jieba.cut_for_search(text)
    words = " ".join(words)
    return words

# 先分词再去除标点符号，是因为标点符号有助于提高分词的精度

# 数据清洗：保留中文字符，替换所有数字、英文字符、标点符号为空格
def clean_text(text):
    cleaned_text = re.sub(r'[^\u4e00-\u9fa5]+', ' ', text)
    return cleaned_text

if __name__ == "__main__":
    preprocess_document(DAT_DOC, DOC_FOLDER)