from requests_html import HTMLSession
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import time
import requests

db_path = 'tourism.db'

# データベースの初期化
def initialize_database(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS tourism_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_name TEXT NOT NULL,
            number_of_foreigners INTEGER NOT NULL
        );
    ''')
    conn.commit()
    conn.close()

# robots.txtの確認
def check_robots_txt(url):
    parsed_url = requests.utils.urlparse(url)
    robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
    try:
        response = requests.get(robots_url)
        if response.status_code == 200:
            print(f"robots.txtの内容:\n{response.text}")
            return response.text
        else:
            print("robots.txtが見つかりませんでした。")
            return ""
    except requests.RequestException as e:
        print(f"robots.txtの取得中にエラーが発生しました: {e}")
        return ""

def is_scraping_allowed(url, robots_txt):
    parsed_url = requests.utils.urlparse(url)
    path = parsed_url.path
    disallowed_paths = [line.split(': ')[1] for line in robots_txt.splitlines() if line.startswith('Disallow')]
    
    for disallowed_path in disallowed_paths:
        if disallowed_path == '/' or path.startswith(disallowed_path):
            return False
            
    return True

# スクレイピング関数
def scrape_tourism_data(url):
    session = HTMLSession()
    response = session.get(url)
    response.html.render(timeout=20)  # JavaScriptをレンダリング
    time.sleep(2)  # 各リクエスト間に休止時間を設ける
    
    soup = BeautifulSoup(response.html.html, 'html.parser')

    data = []
    table = soup.find('table')
    if not table:
        print("テーブルが見つかりませんでした。")
        return data
    
    rows = table.find_all('tr')
    if not rows:
        print("テーブル行が見つかりませんでした。")
        return data
    
    for row in rows[1:]:  # スキップヘッダー行
        th = row.find('th')
        tds = row.find_all('td')
        if th and tds:
            country_name = th.text.strip()
            # ターゲットとなる列から訪問者数を取得
            number_of_foreigners_text = tds[1].get_text(separator=' ', strip=True)

            try:
                # 人数を数値に変換
                number_of_foreigners = int(number_of_foreigners_text.replace('人', '').replace(',', ''))
                data.append({
                    'country_name': country_name,
                    'number_of_foreigners': number_of_foreigners
                })
            except ValueError as e:
                print(f"データのパース中にエラーが発生しました： {e}")
                print(f"エラーが発生した行のHTML: {row}")
                continue

    return data

# データを保存する関数
def store_to_database(data, db_path, table_name='tourism_data'):
    if not data:
        print("保存するデータがありません。")
        return

    df = pd.DataFrame(data)
    engine = create_engine(f'sqlite:///{db_path}')
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"データがテーブル '{table_name}' に保存されました。")

# データベース内容を確認する関数
def check_database(db_path):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM tourism_data", conn)
    print(df.head())
    conn.close()
    return df

# データのプロット
def plot_data(df):
    # フォントの設定
    font_path = '/System/Library/Fonts/ヒラギノ角ゴシック W6.ttc'  # Mac OS上の日本語フォント
    font_prop = fm.FontProperties(fname=font_path)
    
    df.plot(kind='bar', x='country_name', y='number_of_foreigners', legend=False)
    plt.xlabel('国名', fontproperties=font_prop)
    plt.ylabel('訪日外国人数', fontproperties=font_prop)
    plt.title('国別訪日外国人', fontproperties=font_prop)
    
    # x軸のラベルを日本語フォントに設定
    plt.xticks(fontproperties=font_prop, rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

# メイン関数
def main():
    db_path = 'tourism.db'
    initialize_database(db_path)
    url = 'https://www.tourism.jp/tourism-database/stats/inbound/'

    print(f"{url} からデータをスクレイピングします。")
    
    robots_txt = check_robots_txt(url)
    if not is_scraping_allowed(url, robots_txt):
        print("このサイトはスクレイピングが禁止されています。プログラムを終了します。")
        return

    tourism_data = scrape_tourism_data(url)
    if tourism_data:
        store_to_database(tourism_data, db_path)
        df = check_database(db_path)
        plot_data(df)
    else:
        print("データが取得できなかったため、データベースへの保存は行いません。")

if __name__ == "__main__":
    main()