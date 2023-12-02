import chardet

from constants import RAW_TFIDF, PROCESSED_TFIDF

# 处理 MRJob 生成的 TFIDF 倒排索引文档，转换成中文字符，便于查阅

# 识别文件编码格式
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

# 将 Unicode 转义字符转换为中文字符
def convert_unicode_to_chinese(input_path, output_path):
    input_file_encoding = detect_encoding(input_path)
    with open(input_path, 'r', encoding=input_file_encoding) as input_file:
        with open(output_path, 'w', encoding='utf-8') as output_file:
            for line in input_file:
                line = line.encode('utf-8').decode('unicode-escape')                                
                output_file.write(line)
                
if __name__ == "__main__":
    convert_unicode_to_chinese(RAW_TFIDF, PROCESSED_TFIDF)