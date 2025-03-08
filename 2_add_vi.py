import multiprocessing
import time
import xml.etree.ElementTree as ET
import sqlite3
from typing import List, Optional
from tqdm import tqdm
import os
import glob


class Word:
    """Represents a dictionary word entry with its attributes."""

    def __init__(self, word: str, phonetic: Optional[str] = None, mean: Optional[str] = None,
                 is_common: bool = False, priority: Optional[str] = None, info: Optional[str] = None,
                 anki: Optional[str] = None):
        self.word = word.strip() if word else ""
        self.phonetic = phonetic
        self.mean = self._process_mean(mean) if mean else ""
        self.is_common = is_common
        self.priority = priority
        self.info = info
        self.anki = anki

    def _process_mean(self, mean: str) -> str:
        """Extract Vietnamese meanings from the mean string."""
        meanings = [m.strip() for m in mean.split(';')]
        vietnamese_meanings = [m for m in meanings if any(
            '\u00C0' <= c <= '\u1EF9' for c in m)]
        return '; '.join(vietnamese_meanings) if vietnamese_meanings else mean

    def __str__(self) -> str:
        return f"{self.word}\n{self.mean}\n{self.anki}"


class Kanji:
    """Represents a kanji character with its attributes."""

    def __init__(self, kanji: str, hanzi: Optional[str] = None, on: Optional[str] = None,
                 kun: Optional[str] = None, mean: Optional[str] = None, level: Optional[str] = None,
                 priority: Optional[str] = None, info: Optional[str] = None, anki: Optional[str] = None):
        self.kanji = kanji
        self.hanzi = hanzi
        self.on = on
        self.kun = kun
        self.mean = mean
        self.level = level
        self.priority = priority
        self.info = info
        self.anki = anki

    def __str__(self) -> str:
        return f"{self.hanzi}\n{self.anki}"


class StarDict:
    """Represents a StarDict entry with Vietnamese translations."""

    def __init__(self, hanzi: str = "", hanzi_anki: Optional[List[str]] = None,
                 mean: str = "", mean_anki: str = ""):
        self.hanzi = hanzi
        self.hanzi_anki = hanzi_anki if hanzi_anki is not None else []
        self.mean = mean
        self.mean_anki = mean_anki

    def to_xml_sense_elements(self) -> List[ET.Element]:
        """Convert the StarDict entry to XML sense elements for JMdict."""
        sense_elements = []
        if self.mean:
            mean_sense = ET.Element('sense')
            gloss = ET.SubElement(mean_sense, 'gloss')
            gloss.set('xml:lang', 'vi')
            gloss.text = self.mean
            if self.mean_anki:
                misc = ET.SubElement(mean_sense, 'misc')
                misc.text = self.mean_anki
            sense_elements.append(mean_sense)
        if self.hanzi:
            hanzi_sense = ET.Element('sense')
            gloss = ET.SubElement(hanzi_sense, 'gloss')
            gloss.text = self.hanzi
            for anki_entry in self.hanzi_anki:
                misc = ET.SubElement(hanzi_sense, 'misc')
                misc.text = anki_entry
            sense_elements.append(hanzi_sense)
        return sense_elements

    def __str__(self) -> str:
        return f"{self.hanzi}\n{self.hanzi_anki}\n{self.mean}\n{self.mean_anki}"


def get_words(conn: sqlite3.Connection, search_word: str) -> List[Word]:
    """Retrieve words from the database matching the search word exactly or phonetically."""
    try:
        cursor = conn.cursor()
        query = "SELECT word, phonetic, mean, is_common, priority, info, anki FROM words WHERE word = ?"
        cursor.execute(query, (search_word,))
        results = cursor.fetchall()
        if not results:
            query = "SELECT word, phonetic, mean, is_common, priority, info, anki FROM words WHERE phonetic LIKE ?"
            cursor.execute(query, (f'%{search_word}%',))
            results = cursor.fetchall()
        return [Word(word=r[0], phonetic=r[1], mean=r[2], is_common=bool(r[3]) if r[3] is not None else False,
                     priority=r[4], info=r[5], anki=r[6]) for r in results]
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []


def get_kanji(conn: sqlite3.Connection, kanji_char: str) -> Optional[Kanji]:
    """Retrieve a kanji entry from the database by character."""
    try:
        cursor = conn.cursor()
        query = "SELECT kanji, hanzi, onyomi, kunyomi, mean, level, priority, info, anki FROM kanji WHERE kanji = ?"
        cursor.execute(query, (kanji_char,))
        result = cursor.fetchone()
        if result:
            return Kanji(kanji=result[0], hanzi=result[1], on=result[2], kun=result[3], mean=result[4],
                         level=result[5], priority=result[6], info=result[7], anki=result[8])
        return None
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None


def get_kanji_from_word(conn: sqlite3.Connection, word: str) -> List[Kanji]:
    """Get kanji information for each kanji character in the word."""
    kanjis = []
    for char in word:
        if '\u4E00' <= char <= '\u9FFF':  # Kanji Unicode range
            kanji = get_kanji(conn, char)
            if kanji:
                kanjis.append(kanji)
    return kanjis


def filter_word(input_word: str, word_list: List[Word]) -> Optional[Word]:
    """Find the best matching word from a list, preferring exact matches over phonetic."""
    if not word_list:
        return None
    for w in word_list:
        if w.word == input_word:
            return w
    for w in word_list:
        if w.phonetic and input_word in w.phonetic.split(" "):
            return w
    return None


def find_relevant_word(conn: sqlite3.Connection, kanji_elements: List[ET.Element],
                       reading_elements: List[ET.Element]) -> Optional[Word]:
    """Find a relevant word from kanji or reading elements."""
    for k_ele in kanji_elements:
        kanji = k_ele.text
        words = get_words(conn, kanji)
        word = filter_word(kanji, words)
        if word:
            return word
    for r_ele in reading_elements:
        reading = r_ele.text
        words = get_words(conn, reading)
        word = filter_word(reading, words)
        if word:
            return word
    return None


def init_worker(db_path: str) -> None:
    """Initialize each worker process with its own database connection."""
    global db_conn
    db_conn = sqlite3.connect(db_path)


def process_entry(entry_str: str) -> str:
    """Process a single JMdict entry and add Vietnamese translations."""
    entry = ET.fromstring(entry_str)
    kanji_elements = entry.findall('./k_ele/keb')
    reading_elements = entry.findall('./r_ele/reb')

    # Collect kanji data from all kanji elements
    seen_kanji = set()
    kanjis = []
    for k_ele in kanji_elements:
        kanji_text = k_ele.text
        for kanji in get_kanji_from_word(db_conn, kanji_text):
            if kanji.kanji not in seen_kanji:
                seen_kanji.add(kanji.kanji)
                kanjis.append(kanji)

    # Find the relevant word
    word = find_relevant_word(db_conn, kanji_elements, reading_elements)

    # Create StarDict entry
    stardict = StarDict(
        hanzi=" | ".join([k.hanzi for k in kanjis if k.hanzi]),
        hanzi_anki=[f"{k.kanji} : {k.anki}" for k in kanjis if k.anki],
        mean=word.mean if word else "",
        mean_anki=word.anki if word else ""
    )

    # Insert new sense elements
    xml_sense_elements = stardict.to_xml_sense_elements()
    if xml_sense_elements:
        existing_senses = entry.findall('./sense')
        if existing_senses:
            first_sense_index = list(entry).index(existing_senses[0])
            for elem in reversed(xml_sense_elements):
                entry.insert(first_sense_index, elem)
        else:
            for elem in xml_sense_elements:
                entry.append(elem)

    # print(f"Processed entry: {word.word if word else 'N/A'}")
    return ET.tostring(entry, encoding='unicode')


def update_jmdict_with_vietnamese_parallel(jmdict_path: str, output_path: str, db_path: str,
                                           num_processes: Optional[int] = None) -> None:
    """Update JMdict XML with Vietnamese translations using parallel processing."""
    start_time = time.time()  # Start timing

    print(f"Reading JMdict XML from {jmdict_path}...")
    tree = ET.parse(jmdict_path)
    root = tree.getroot()
    entries = root.findall('.//entry')
    total_entries = len(entries)
    print(f"Found {total_entries} entries to process...")

    entry_strs = [ET.tostring(entry, encoding='unicode') for entry in entries]

    print("Processing entries in parallel...")
    with multiprocessing.Pool(processes=num_processes, initializer=init_worker, initargs=(db_path,)) as pool:
        modified_entry_strs = list(tqdm(
            pool.imap(process_entry, entry_strs),
            total=total_entries,
            desc="Processing entries",
            unit="entries"
        ))

    print("Updating XML tree with processed entries...")
    for i, modified_str in tqdm(enumerate(modified_entry_strs), total=len(modified_entry_strs), desc="Updating XML"):
        try:
            root[i] = ET.fromstring(modified_str)
        except ET.ParseError as e:
            print(f"Error parsing entry {i}: {e}")
            root[i] = entries[i]  # Keep original entry if parsing fails

    print(f"Writing modified XML to {output_path}...")
    tree.write(output_path, encoding='utf-8', xml_declaration=True)

    end_time = time.time()
    execution_time = end_time - start_time
    print(
        f"Processing completed successfully in {execution_time:.2f} seconds!")
    print(
        f"Average time per entry: {(execution_time/total_entries):.3f} seconds")


if __name__ == "__main__":

    db_path = "dict.db"
    input_folder = "jmdict_parts"
    output_folder = "jmdict_vi"

    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Get all XML files in input folder
    xml_files = glob.glob(os.path.join(input_folder, "jmdict_part_*.xml"))

    print(f"Found {len(xml_files)} XML files to process")

    for input_path in xml_files:
        # Generate output filename
        filename = os.path.basename(input_path)
        output_path = os.path.join(output_folder, filename)

        # Skip if output file already exists
        if os.path.exists(output_path):
            print(f"Skipping {filename} - output already exists")
            continue

        print(f"\nProcessing {filename}...")
        update_jmdict_with_vietnamese_parallel(
            input_path, output_path, db_path, num_processes=12)
