import os

from constants import PROCESSED_TFIDF, DOC_FOLDER, TOP_K

def search_documents(query, data_file, data_folder, topk):
    results = []

    with open(data_file, 'r', encoding='utf-8') as input_file:
        for line in input_file:
            word, doc_tfidfs = line.strip().split('\t')
            word = word.strip('"')

            if query == word:
                
                # 解析文档和tfidf值
                doc_tfidfs = eval(doc_tfidfs)
                
                # 按tfidf值降序排序
                sorted_docs = sorted(doc_tfidfs, key=lambda x: x[1], reverse=True)
                
                # 提取topk个文档
                topk_docs = [doc[0] for doc in sorted_docs[:topk]]
                
                for rank, docid in enumerate(topk_docs, start=1):
                    doc_filename = f"{docid}"
                    doc_filepath = os.path.join(data_folder, doc_filename)

                    if os.path.exists(doc_filepath):
                        with open(doc_filepath, 'r', encoding='utf-8') as doc_file:
                            doc_name = doc_file.readline().strip()
                            results.append(f"{rank}. {docid}  {doc_name}")

    return results

if __name__ == "__main__":
    while(1):
        query_word = input("\n请输入查询的关键词: ")
        topk_results = search_documents(query_word, PROCESSED_TFIDF, DOC_FOLDER, TOP_K)
        if len(topk_results) == 0:
            print("您查找的关键词尚不存在")
        else:
            print(f"与关键词 '{query_word}' 相关的文档，按照相关性排列如下：")
            for result in topk_results:
                print(result)