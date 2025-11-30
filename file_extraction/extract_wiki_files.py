import bz2
import json
from io import BufferedReader
from pathlib import Path
import re
import xml.etree.ElementTree as ETree

INF = 1<<30
EXTRACT_FILES = 3000

def main():
    with Path("./block_list.txt").open("r") as f:
        positions = list(map(int, f.readlines()))
    
    with Path("./id_list.txt").open("r") as f:
        records = f.readlines()
        indicies_by_position: dict[int, list[int]] = {}
        for record in records:
            position, idx = map(int, record.split())
            # 初出のポジションが現れたら初期化
            if position not in indicies_by_position:
                indicies_by_position[position] = []

            indicies_by_position[position].append(idx)

    dataset = Path("./enwiki-20251101-pages-articles-multistream.xml.bz2").open("rb")
    file_count = 0
    for start, end in zip(positions[:-1], positions[1:]):
        # 必要数抽出したらループを抜ける
        if file_count >= EXTRACT_FILES:
            break

        block = extract_block(dataset, start, end)
        _xml = bz2.decompress(block).decode(encoding="UTF-8")
        xml = f"<root>{_xml}</root>"
        limit = EXTRACT_FILES - file_count
        extracted_file_cnt = extract_page_texts(xml, indicies_by_position[start], limit)
        file_count += extracted_file_cnt

    dataset.close()


def extract_block(dataset: BufferedReader, s_position: int, e_position: int) -> bytes:
    dataset.seek(s_position)
    data = dataset.read(e_position - s_position)
    return data

def extract_page_texts(xml: str, indicies: list[int], limit: int = INF) -> int:
    extracted_cnt = 0
    root = ETree.fromstring(xml)
    for idx in indicies:
        if extracted_cnt == limit:
            break

        page = root.find(f"page/[id='{idx}']")
        title = page.find("title").text
        text = page.find("revision/text").text

        # データが空 or REDIRECTの場合はスキップ
        stop_pattern = re.compile("^#REDIRECT", flags=re.IGNORECASE)
        if text is None or stop_pattern.match(text) is not None:
            continue

        with Path(f"wiki3000/wiki_{str(idx).zfill(5)}.json").open("w") as f:
            content = {
                "id": str(idx),
                "title": title,
                "text": text
            }
            json.dump(content, f)
            extracted_cnt += 1

    return extracted_cnt

if __name__ == "__main__":
    main()
