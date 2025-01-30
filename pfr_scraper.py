import re
import os
import time
import json
import string
import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

pd.set_option('mode.copy_on_write', True)

BASE_URL = 'https://www.pro-football-reference.com/{0}'
PLAYER_LIST_URL = 'https://www.pro-football-reference.com/players/{0}'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

PLAYER_LIST_PATH = 'data/player_list.csv'
PLAYER_STATS_PATH = 'data/player_stats.csv'

SCRAPING_RATE = 10 # pages per minute

def scrape_page(url, max_retries = 10, backoff_factor = 10, headers = HEADERS, timeout = 5):
    """
    Scrapes a web page and retries on timeout.

    :param url: URL of the web page to scrape
    :param max_retries: Maximum number of retry attempts
    :param backoff_factor: Backoff factor for retries. Algorithm for waiting between retries: {backoff factor} * (2 ** ({number of total retries} - 1))
    :param headers: HTTP headers to include in the request
    :param timeout: Number of seconds to wait for the server to send data before giving up
    :return: The HTML content of the web page, or None if all retries fail
    """
    retry_strategy = Retry(
        total = max_retries,
        backoff_factor = backoff_factor,
        status_forcelist = [429, 500, 502, 503, 504],
        allowed_methods = ["GET"]
    )
    
    adapter = HTTPAdapter(max_retries = retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    try:
        response = session.get(url, headers = headers, timeout = timeout)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Failed to scrape {url}: {e}")
        return None
        
def scrape_player_lists(urls, max_pages_per_minute = SCRAPING_RATE):
    """
    Scrapes multiple web pages, pausing between each page to avoid exceeding the rate limit.

    :param urls: List of URLs of the web pages to scrape
    :param max_pages_per_minute: Maximum number of pages to scrape per minute
    :return: List of HTML content of the web pages
    """
    data = []
    delay = 60 / max_pages_per_minute
    for url in urls:
        print(f"Scraping {url}")
        response = scrape_page(url)
        if response is not None:
            player_data = parse_player_list_page(response)
            data.append(player_data)
        else:
            print("Failed to scrape page.")
        time.sleep(delay)
    return data

def parse_player_list_page(player_list_page):
    """"
    Parses the player list page and returns a DataFrame with player data.

    :param player_list: Response object from scraping the player list page
    :return: DataFrame with player data
    """
    soup = BeautifulSoup(player_list_page.content, 'html.parser')
    player_list = soup.find('div', {'id': 'div_players'}).find_all('p')
    
    data = []
    for entry in player_list:
        # Extract link and name
        a_tag = entry.find('a')
        link = a_tag['href']
        name = a_tag.text

        # Extract position
        position = entry.text.split(')')[0].split('(')[-1]

        # Extract career years
        years_text = entry.text.split(')')[-1].strip()
        career_begin, career_end = map(int, years_text.split('-'))

        # Extract active status
        active = bool(entry.find('b'))
        data.append([link, name, position, career_begin, career_end, active])

    df = pd.DataFrame(data, columns=['link', 'name', 'position', 'career_begin', 'career_end', 'active'])
    df['scraped'] = False
    return df

def parse_player_stats_page(player_page):
    """
    Parses the player stats page and returns a DataFrame with player stats.
    
    :param player_page: Response object from scraping the player's individual page
    """
    soup = BeautifulSoup(player_page.content, 'html.parser')

    meta_info = soup.find('div', {'id': 'meta'}).text
    clean_meta_info = re.sub(r"\s+", " ", meta_info)

    # Search for height and weight in the meta info
    pattern = r"(\d+-\d+), (\d+lb)"
    match = re.search(pattern, clean_meta_info)
    if match:
        height = match.group(1)
        weight = match.group(2)
    else:
        height = None
        weight = None
        
    soup = BeautifulSoup(response.content, 'html.parser')

    meta_info = soup.find('div', {'id': 'meta'}).text
    clean_meta_info = re.sub(r"\s+", " ", meta_info)

    # Search for height and weight in the meta info
    pattern = r"(\d+-\d+), (\d+lb)"
    match = re.search(pattern, clean_meta_info)
    if match:
        height = match.group(1)
        weight = match.group(2)
    else:
        height = None
        weight = None

    player_games_reg_g = 0
    player_games_started_reg_g = 0

    games_reg = soup.find('table', {'id': 'games_played'})
    if games_reg is not None:

        games_reg_g = games_reg.find('tfoot').find_all('td', {'data-stat': 'g'})
        if games_reg_g is not None and len(games_reg_g) > 0:
            player_games_reg_g = int(games_reg_g[0].get_text())
            
        games_started_reg_g = games_reg.find('tfoot').find_all('td', {'data-stat': 'gs'})
        if games_started_reg_g is not None and len(games_started_reg_g) > 0:
            if games_started_reg_g[0].get_text() == '':
                player_games_started_reg_g = 0
            else:
                player_games_started_reg_g = int(games_started_reg_g[0].get_text())
            
    player_games_post_g = 0
    player_games_started_post_g = 0

    games_post = soup.find('table', {'id': 'games_played_playoffs'})
    if games_post is not None:

        games_post_g = games_post.find('tfoot').find_all('td', {'data-stat': 'g'})
        if games_post_g is not None and len(games_post_g) > 0:
            player_games_post_g = int(games_post_g[0].get_text())
            
        games_started_post_g = games_post.find('tfoot').find_all('td', {'data-stat': 'gs'})
        if games_started_post_g is not None and len(games_started_post_g) > 0:
            if games_started_post_g[0].get_text() == '':
                player_games_started_post_g = 0
            else:
                player_games_started_post_g = int(games_started_post_g[0].get_text())

    player_games_reg_p = 0
    player_games_started_reg_p = 0
    player_qb_rec_reg = 0
    player_pass_cmp_reg = 0
    player_pass_att_reg = 0
    player_pass_cmp_pct_reg = 0
    player_pass_yds_reg = 0
    player_pass_td_reg = 0
    player_pass_td_pct_reg = 0
    player_pass_int_reg = 0
    player_pass_int_pct_reg = 0
    player_pass_first_down_reg = 0
    player_pass_success_reg = 0
    player_pass_long_reg = 0
    player_pass_yds_per_att_reg = 0
    player_pass_adj_yds_per_att_reg = 0
    player_pass_yds_per_cmp_reg = 0
    player_pass_yds_per_g_reg = 0
    player_pass_rating_reg = 0
    player_pass_sacked_reg = 0
    player_pass_sacked_yds_reg = 0
    player_pass_sacked_pct_reg = 0
    player_pass_net_yds_per_att_reg = 0
    player_pass_adj_net_yds_per_att_reg = 0
    player_comebacks_reg = 0
    player_gwd_reg = 0

    passing_reg = soup.find('table', {'id': 'passing'})
    if passing_reg is not None:
        
        games_reg_p = passing_reg.find('tfoot').find_all('td', {'data-stat': 'games'})
        if games_reg_p is not None and len(games_reg_p) > 0:
            player_games_reg_p = int(games_reg_p[0].get_text())
            
        games_started_reg_rr = passing_reg.find('tfoot').find_all('td', {'data-stat': 'games_started'})
        if games_started_reg_rr is not None and len(games_started_reg_rr) > 0:
            player_games_started_reg_p = int(games_started_reg_rr[0].get_text())
            
        qb_rec_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'qb_rec'})
        if qb_rec_reg is not None and len(qb_rec_reg) > 0:
            player_qb_rec_reg = qb_rec_reg[0].get_text()
            
        pass_cmp_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_cmp'})
        if pass_cmp_reg is not None and len(pass_cmp_reg) > 0:
            player_pass_cmp_reg = int(pass_cmp_reg[0].get_text())
            
        pass_att_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_att'})
        if pass_att_reg is not None and len(pass_att_reg) > 0:
            player_pass_att_reg = int(pass_att_reg[0].get_text())
            
        pass_cmp_pct_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_cmp_pct'})
        if pass_cmp_pct_reg is not None and len(pass_cmp_pct_reg) > 0:
            player_pass_cmp_pct_reg = float(pass_cmp_pct_reg[0].get_text())
            
        pass_yds_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_yds'})
        if pass_yds_reg is not None and len(pass_yds_reg) > 0:
            player_pass_yds_reg = int(pass_yds_reg[0].get_text())
            
        pass_td_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_td'})
        if pass_td_reg is not None and len(pass_td_reg) > 0:
            player_pass_td_reg = int(pass_td_reg[0].get_text())
            
        pass_td_pct_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_td_pct'})
        if pass_td_pct_reg is not None and len(pass_td_pct_reg) > 0:
            player_pass_td_pct_reg = float(pass_td_pct_reg[0].get_text())
            
        pass_int_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_int'})
        if pass_int_reg is not None and len(pass_int_reg) > 0:
            player_pass_int_reg = int(pass_int_reg[0].get_text())
            
        pass_int_pct_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_int_pct'})
        if pass_int_pct_reg is not None and len(pass_int_pct_reg) > 0:
            player_pass_int_pct_reg = float(pass_int_pct_reg[0].get_text())
            
        pass_first_down_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_first_down'})
        if pass_first_down_reg is not None and len(pass_first_down_reg) > 0:
            player_pass_first_down_reg = int(pass_first_down_reg[0].get_text())
            
        pass_success_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_success'})
        if pass_success_reg is not None and len(pass_success_reg) > 0:
            player_pass_success_reg = float(pass_success_reg[0].get_text())
            
        pass_long_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_long'})
        if pass_long_reg is not None and len(pass_long_reg) > 0:
            player_pass_long_reg = int(pass_long_reg[0].get_text())
            
        pass_yds_per_att_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_yds_per_att'})
        if pass_yds_per_att_reg is not None and len(pass_yds_per_att_reg) > 0:
            player_pass_yds_per_att_reg = float(pass_yds_per_att_reg[0].get_text())
            
        pass_adj_yds_per_att_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_adj_yds_per_att'})
        if pass_adj_yds_per_att_reg is not None and len(pass_adj_yds_per_att_reg) > 0:
            player_pass_adj_yds_per_att_reg = float(pass_adj_yds_per_att_reg[0].get_text())
            
        pass_yds_per_cmp_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_yds_per_cmp'})
        if pass_yds_per_cmp_reg is not None and len(pass_yds_per_cmp_reg) > 0:
            player_pass_yds_per_cmp_reg = float(pass_yds_per_cmp_reg[0].get_text())
            
        pass_yds_per_g_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_yds_per_g'})
        if pass_yds_per_g_reg is not None and len(pass_yds_per_g_reg) > 0:
            player_pass_yds_per_g_reg = float(pass_yds_per_g_reg[0].get_text())
            
        pass_rating_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_rating'})
        if pass_rating_reg is not None and len(pass_rating_reg) > 0:
            player_pass_rating_reg = float(pass_rating_reg[0].get_text())
            
        pass_sacked_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_sacked'})
        if pass_sacked_reg is not None and len(pass_sacked_reg) > 0:
            player_pass_sacked_reg = int(pass_sacked_reg[0].get_text())
            
        pass_sacked_yds_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_sacked_yds'})
        if pass_sacked_yds_reg is not None and len(pass_sacked_yds_reg) > 0:
            player_pass_sacked_yds_reg = int(pass_sacked_yds_reg[0].get_text())
            
        pass_sacked_pct_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_sacked_pct'})
        if pass_sacked_pct_reg is not None and len(pass_sacked_pct_reg) > 0:
            player_pass_sacked_pct_reg = float(pass_sacked_pct_reg[0].get_text())
            
        pass_net_yds_per_att_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_net_yds_per_att'})
        if pass_net_yds_per_att_reg is not None and len(pass_net_yds_per_att_reg) > 0:
            player_pass_net_yds_per_att_reg = float(pass_net_yds_per_att_reg[0].get_text())
            
        pass_adj_net_yds_per_att_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_adj_net_yds_per_att'})
        if pass_adj_net_yds_per_att_reg is not None and len(pass_adj_net_yds_per_att_reg) > 0:
            player_pass_adj_net_yds_per_att_reg = float(pass_adj_net_yds_per_att_reg[0].get_text())
            
        pass_adj_net_yds_per_att_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'pass_adj_net_yds_per_att'})
        if pass_adj_net_yds_per_att_reg is not None and len(pass_adj_net_yds_per_att_reg) > 0:
            player_pass_adj_net_yds_per_att_reg = float(pass_adj_net_yds_per_att_reg[0].get_text())
            
        comebacks_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'comebacks'})
        if comebacks_reg is not None and len(comebacks_reg) > 0:
            player_comebacks_reg = int(comebacks_reg[0].get_text())
            
        gwd_reg = passing_reg.find('tfoot').find_all('td', {'data-stat': 'gwd'})
        if gwd_reg is not None and len(gwd_reg) > 0:
            player_gwd_reg = int(gwd_reg[0].get_text())

    player_games_post_p = 0
    player_games_started_post_p = 0
    player_qb_rec_post = 0
    player_pass_cmp_post = 0
    player_pass_att_post = 0
    player_pass_cmp_pct_post = 0
    player_pass_yds_post = 0
    player_pass_td_post = 0
    player_pass_td_pct_post = 0
    player_pass_int_post = 0
    player_pass_int_pct_post = 0
    player_pass_first_down_post = 0
    player_pass_success_post = 0
    player_pass_long_post = 0
    player_pass_yds_per_att_post = 0
    player_pass_adj_yds_per_att_post = 0
    player_pass_yds_per_cmp_post = 0
    player_pass_yds_per_g_post = 0
    player_pass_rating_post = 0
    player_pass_sacked_post = 0
    player_pass_sacked_yds_post = 0
    player_pass_sacked_pct_post = 0
    player_pass_net_yds_per_att_post = 0
    player_pass_adj_net_yds_per_att_post = 0
    player_comebacks_post = 0
    player_gwd_post = 0

    passing_post = soup.find('table', {'id': 'passing_post'})
    if passing_post is not None:
        
        games_post_p = passing_post.find('tfoot').find_all('td', {'data-stat': 'games'})
        if games_post_p is not None and len(games_post_p) > 0:
            player_games_post_p = int(games_post_p[0].get_text())
            
        games_started_post_p = passing_post.find('tfoot').find_all('td', {'data-stat': 'games_started'})
        if games_started_post_p is not None and len(games_started_post_p) > 0:
            player_games_started_post_p = int(games_started_post_p[0].get_text())
            
        qb_rec_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'qb_rec'})
        if qb_rec_post is not None and len(qb_rec_post) > 0:
            player_qb_rec_post = qb_rec_post[0].get_text()
            
        pass_cmp_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_cmp'})
        if pass_cmp_post is not None and len(pass_cmp_post) > 0:
            player_pass_cmp_post = int(pass_cmp_post[0].get_text())
            
        pass_att_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_att'})
        if pass_att_post is not None and len(pass_att_post) > 0:
            player_pass_att_post = int(pass_att_post[0].get_text())
            
        pass_cmp_pct_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_cmp_pct'})
        if pass_cmp_pct_post is not None and len(pass_cmp_pct_post) > 0:
            player_pass_cmp_pct_post = float(pass_cmp_pct_post[0].get_text())
            
        pass_yds_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_yds'})
        if pass_yds_post is not None and len(pass_yds_post) > 0:
            player_pass_yds_post = int(pass_yds_post[0].get_text())
            
        pass_td_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_td'})
        if pass_td_post is not None and len(pass_td_post) > 0:
            player_pass_td_post = int(pass_td_post[0].get_text())
            
        pass_td_pct_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_td_pct'})
        if pass_td_pct_post is not None and len(pass_td_pct_post) > 0:
            player_pass_td_pct_post = float(pass_td_pct_post[0].get_text())
            
        pass_int_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_int'})
        if pass_int_post is not None and len(pass_int_post) > 0:
            player_pass_int_post = int(pass_int_post[0].get_text())
            
        pass_int_pct_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_int_pct'})
        if pass_int_pct_post is not None and len(pass_int_pct_post) > 0:
            player_pass_int_pct_post = float(pass_int_pct_post[0].get_text())
            
        pass_first_down_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_first_down'})
        if pass_first_down_post is not None and len(pass_first_down_post) > 0:
            player_pass_first_down_post = int(pass_first_down_post[0].get_text())
            
        pass_success_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_success'})
        if pass_success_post is not None and len(pass_success_post) > 0:
            player_pass_success_post = float(pass_success_post[0].get_text())
            
        pass_long_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_long'})
        if pass_long_post is not None and len(pass_long_post) > 0:
            player_pass_long_post = int(pass_long_post[0].get_text())
            
        pass_yds_per_att_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_yds_per_att'})
        if pass_yds_per_att_post is not None and len(pass_yds_per_att_post) > 0:
            player_pass_yds_per_att_post = float(pass_yds_per_att_post[0].get_text())
            
        pass_adj_yds_per_att_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_adj_yds_per_att'})
        if pass_adj_yds_per_att_post is not None and len(pass_adj_yds_per_att_post) > 0:
            player_pass_adj_yds_per_att_post = float(pass_adj_yds_per_att_post[0].get_text())
            
        pass_yds_per_cmp_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_yds_per_cmp'})
        if pass_yds_per_cmp_post is not None and len(pass_yds_per_cmp_post) > 0:
            player_pass_yds_per_cmp_post = float(pass_yds_per_cmp_post[0].get_text())
            
        pass_yds_per_g_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_yds_per_g'})
        if pass_yds_per_g_post is not None and len(pass_yds_per_g_post) > 0:
            player_pass_yds_per_g_post = float(pass_yds_per_g_post[0].get_text())
            
        pass_rating_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_rating'})
        if pass_rating_post is not None and len(pass_rating_post) > 0:
            player_pass_rating_post = float(pass_rating_post[0].get_text())
            
        pass_sacked_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_sacked'})
        if pass_sacked_post is not None and len(pass_sacked_post) > 0:
            player_pass_sacked_post = int(pass_sacked_post[0].get_text())
            
        pass_sacked_yds_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_sacked_yds'})
        if pass_sacked_yds_post is not None and len(pass_sacked_yds_post) > 0:
            player_pass_sacked_yds_post = int(pass_sacked_yds_post[0].get_text())
            
        pass_sacked_pct_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_sacked_pct'})
        if pass_sacked_pct_post is not None and len(pass_sacked_pct_post) > 0:
            player_pass_sacked_pct_post = float(pass_sacked_pct_post[0].get_text())
            
        pass_net_yds_per_att_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_net_yds_per_att'})
        if pass_net_yds_per_att_post is not None and len(pass_net_yds_per_att_post) > 0:
            player_pass_net_yds_per_att_post = float(pass_net_yds_per_att_post[0].get_text())
            
        pass_adj_net_yds_per_att_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_adj_net_yds_per_att'})
        if pass_adj_net_yds_per_att_post is not None and len(pass_adj_net_yds_per_att_post) > 0:
            player_pass_adj_net_yds_per_att_post = float(pass_adj_net_yds_per_att_post[0].get_text())
            
        pass_adj_net_yds_per_att_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'pass_adj_net_yds_per_att'})
        if pass_adj_net_yds_per_att_post is not None and len(pass_adj_net_yds_per_att_post) > 0:
            player_pass_adj_net_yds_per_att_post = float(pass_adj_net_yds_per_att_post[0].get_text())
            
        comebacks_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'comebacks'})
        if comebacks_post is not None and len(comebacks_post) > 0:
            player_comebacks_post = int(comebacks_post[0].get_text())
            
        gwd_post = passing_post.find('tfoot').find_all('td', {'data-stat': 'gwd'})
        if gwd_post is not None and len(gwd_post) > 0:
            player_gwd_post = int(gwd_post[0].get_text())

    player_games_reg_rr = 0
    player_games_started_reg_rr = 0
    player_rush_att_reg = 0
    player_rush_yds_reg = 0
    player_rush_td_reg = 0
    player_rush_first_down_reg = 0
    player_rush_success_reg = 0
    player_rush_long_reg = 0
    player_rush_yds_per_att_reg = 0
    player_rush_yds_per_g_reg = 0
    player_rush_att_per_g_reg = 0
    player_targets_reg = 0
    player_rec_reg = 0
    player_rec_yds_reg = 0
    player_rec_yds_per_rec_reg = 0
    player_rec_td_reg = 0
    player_rec_first_down_reg = 0
    player_rec_success_reg = 0
    player_rec_long_reg = 0
    player_rec_per_g_reg = 0
    player_rec_yds_per_g_reg = 0
    player_catch_pct_reg = 0
    player_rec_yds_per_tgt_reg = 0
    player_touches_reg = 0
    player_yds_per_touch_reg = 0
    player_rush_receive_td_reg = 0

    rushing_and_receiving_reg = soup.find('table', {'id': 'rushing_and_receiving'})
    if rushing_and_receiving_reg is None:
        rushing_and_receiving_reg = soup.find('table', {'id': 'receiving_and_rushing'})
    if rushing_and_receiving_reg is not None:

        games_reg_rr = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'games'})
        if games_reg_rr is not None and len(games_reg_rr) > 0:
            player_games_reg_rr = int(games_reg_rr[0].get_text())
            
        games_started_reg_rr = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'games_started'})
        if games_started_reg_rr is not None and len(games_started_reg_rr) > 0:
            player_games_started_reg_rr = int(games_started_reg_rr[0].get_text())
            
        rush_att_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rush_att'})
        if rush_att_reg is not None and len(rush_att_reg) > 0:
            player_rush_att_reg = int(rush_att_reg[0].get_text())
            
        rush_yds_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rush_yds'})
        if rush_yds_reg is not None and len(rush_yds_reg) > 0:
            player_rush_yds_reg = int(rush_yds_reg[0].get_text())

        rush_td_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rush_td'})
        if rush_td_reg is not None and len(rush_td_reg) > 0:
            player_rush_td_reg = int(rush_td_reg[0].get_text())

        rush_first_down_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rush_first_down'})
        if rush_first_down_reg is not None and len(rush_first_down_reg) > 0:
            player_rush_first_down_reg = int(rush_first_down_reg[0].get_text())

        rush_success_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rush_success'})
        if rush_success_reg is not None and len(rush_success_reg) > 0:
            player_rush_success_reg = float(rush_success_reg[0].get_text())
            
        rush_long_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rush_long'})
        if rush_long_reg is not None and len(rush_long_reg) > 0:
            player_rush_long_reg = int(rush_long_reg[0].get_text())
            
        rush_yds_per_att_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rush_yds_per_att'})
        if rush_yds_per_att_reg is not None and len(rush_yds_per_att_reg) > 0:
            player_rush_yds_per_att_reg = float(rush_yds_per_att_reg[0].get_text())
            
        rush_yds_per_g_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rush_yds_per_g'})
        if rush_yds_per_g_reg is not None and len(rush_yds_per_g_reg) > 0:
            player_rush_yds_per_g_reg = float(rush_yds_per_g_reg[0].get_text())
            
        rush_att_per_g_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rush_att_per_g'})
        if rush_att_per_g_reg is not None and len(rush_att_per_g_reg) > 0:
            player_rush_att_per_g_reg = float(rush_att_per_g_reg[0].get_text()) 
            
        targets_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'targets'})
        if targets_reg is not None and len(targets_reg) > 0:
            player_targets_reg = int(targets_reg[0].get_text())
            
        rec_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rec'})
        if rec_reg is not None and len(rec_reg) > 0:
            player_rec_reg = int(rec_reg[0].get_text())
            
        rec_yds_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rec_yds'})
        if rec_yds_reg is not None and len(rec_yds_reg) > 0:
            player_rec_yds_reg = int(rec_yds_reg[0].get_text())
            
        rec_yds_per_rec_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rec_yds_per_rec'})
        if rec_yds_per_rec_reg is not None and len(rec_yds_per_rec_reg) > 0:
            player_rec_yds_per_rec_reg = float(rec_yds_per_rec_reg[0].get_text())

        rec_td_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rec_td'})
        if rec_td_reg is not None and len(rec_td_reg) > 0:
            player_rec_td_reg = int(rec_td_reg[0].get_text())
            
        rec_first_down_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rec_first_down'})
        if rec_first_down_reg is not None and len(rec_first_down_reg) > 0:
            player_rec_first_down_reg = int(rec_first_down_reg[0].get_text())
            
        rec_success_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rec_success'})
        if rec_success_reg is not None and len(rec_success_reg) > 0:
            player_rec_success_reg = float(rec_success_reg[0].get_text())
            
        rec_long_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rec_long'})
        if rec_long_reg is not None and len(rec_long_reg) > 0:
            player_rec_long_reg = int(rec_long_reg[0].get_text())
            
        rec_per_g_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rec_per_g'})
        if rec_per_g_reg is not None and len(rec_per_g_reg) > 0:
            player_rec_per_g_reg = float(rec_per_g_reg[0].get_text())
            
        rec_yds_per_g_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rec_yds_per_g'})
        if rec_yds_per_g_reg is not None and len(rec_yds_per_g_reg) > 0:
            player_rec_yds_per_g_reg = float(rec_yds_per_g_reg[0].get_text())
            
        catch_pct_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'catch_pct'})
        if catch_pct_reg is not None and len(catch_pct_reg) > 0:
            player_catch_pct_reg = float(catch_pct_reg[0].get_text())
            
        rec_yds_per_tgt_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rec_yds_per_tgt'})
        if rec_yds_per_tgt_reg is not None and len(rec_yds_per_tgt_reg) > 0:
            player_rec_yds_per_tgt_reg = float(rec_yds_per_tgt_reg[0].get_text())
            
        touches_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'touches'})
        if touches_reg is not None and len(touches_reg) > 0:
            player_touches_reg = int(touches_reg[0].get_text())
            
        yds_per_touch_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'yds_per_touch'})
        if yds_per_touch_reg is not None and len(yds_per_touch_reg) > 0:
            player_yds_per_touch_reg = float(yds_per_touch_reg[0].get_text())
            
        rush_receive_td_reg = rushing_and_receiving_reg.find('tfoot').find_all('td', {'data-stat': 'rush_receive_td'})
        if rush_receive_td_reg is not None and len(rush_receive_td_reg) > 0:
            player_rush_receive_td_reg = int(rush_receive_td_reg[0].get_text())

    player_games_post_rr = 0
    player_games_started_post_rr = 0
    player_rush_att_post = 0
    player_rush_yds_post = 0
    player_rush_td_post = 0
    player_rush_first_down_post = 0
    player_rush_success_post = 0
    player_rush_long_post = 0
    player_rush_yds_per_att_post = 0
    player_rush_yds_per_g_post = 0
    player_rush_att_per_g_post = 0
    player_targets_post = 0
    player_rec_post = 0
    player_rec_yds_post = 0
    player_rec_yds_per_rec_post = 0
    player_rec_td_post = 0
    player_rec_first_down_post = 0
    player_rec_success_post = 0
    player_rec_long_post = 0
    player_rec_per_g_post = 0
    player_rec_yds_per_g_post = 0
    player_catch_pct_post = 0
    player_rec_yds_per_tgt_post = 0
    player_touches_post = 0
    player_yds_per_touch_post = 0
    player_rush_receive_td_post = 0

    rushing_and_receiving_post = soup.find('table', {'id': 'rushing_and_receiving_post'})
    if rushing_and_receiving_reg is None:
        rushing_and_receiving_post = soup.find('table', {'id': 'receiving_and_rushing_post'})
    if rushing_and_receiving_post is not None:
        
        games_post_rr = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'games'})
        if games_post_rr is not None and len(games_post_rr) > 0:
            player_games_post_rr = int(games_post_rr[0].get_text())
            
        games_started_post_rr = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'games_started'})
        if games_started_post_rr is not None and len(games_started_post_rr) > 0:
            player_games_started_post_rr = int(games_started_post_rr[0].get_text())

        rush_att_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rush_att'})
        if rush_att_post is not None and len(rush_att_post) > 0:
            player_rush_att_post = int(rush_att_post[0].get_text())
            
        rush_yds_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rush_yds'})
        if rush_yds_post is not None and len(rush_yds_post) > 0:
            player_rush_yds_post = int(rush_yds_post[0].get_text())

        rush_td_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rush_td'})
        if rush_td_post is not None and len(rush_td_post) > 0:
            player_rush_td_post = int(rush_td_post[0].get_text())

        rush_first_down_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rush_first_down'})
        if rush_first_down_post is not None and len(rush_first_down_post) > 0:
            player_rush_first_down_post = int(rush_first_down_post[0].get_text())

        rush_success_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rush_success'})
        if rush_success_post is not None and len(rush_success_post) > 0:
            player_rush_success_post = float(rush_success_post[0].get_text())
            
        rush_long_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rush_long'})
        if rush_long_post is not None and len(rush_long_post) > 0:
            player_rush_long_post = int(rush_long_post[0].get_text())
            
        rush_yds_per_att_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rush_yds_per_att'})
        if rush_yds_per_att_post is not None and len(rush_yds_per_att_post) > 0:
            player_rush_yds_per_att_post = float(rush_yds_per_att_post[0].get_text())
            
        rush_yds_per_g_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rush_yds_per_g'})
        if rush_yds_per_g_post is not None and len(rush_yds_per_g_post) > 0:
            player_rush_yds_per_g_post = float(rush_yds_per_g_post[0].get_text())
            
        rush_att_per_g_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rush_att_per_g'})
        if rush_att_per_g_post is not None and len(rush_att_per_g_post) > 0:
            player_rush_att_per_g_post = float(rush_att_per_g_post[0].get_text()) 
            
        targets_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'targets'})
        if targets_post is not None and len(targets_post) > 0:
            player_targets_post = int(targets_post[0].get_text())
            
        rec_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rec'})
        if rec_post is not None and len(rec_post) > 0:
            player_rec_post = int(rec_post[0].get_text())
            
        rec_yds_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rec_yds'})
        if rec_yds_post is not None and len(rec_yds_post) > 0:
            player_rec_yds_post = int(rec_yds_post[0].get_text())
            
        rec_yds_per_rec_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rec_yds_per_rec'})
        if rec_yds_per_rec_post is not None and len(rec_yds_per_rec_post) > 0:
            player_rec_yds_per_rec_post = float(rec_yds_per_rec_post[0].get_text())

        rec_td_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rec_td'})
        if rec_td_post is not None and len(rec_td_post) > 0:
            player_rec_td_post = int(rec_td_post[0].get_text())
            
        rec_first_down_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rec_first_down'})
        if rec_first_down_post is not None and len(rec_first_down_post) > 0:
            player_rec_first_down_post = int(rec_first_down_post[0].get_text())
            
        rec_success_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rec_success'})
        if rec_success_post is not None and len(rec_success_post) > 0:
            player_rec_success_post = float(rec_success_post[0].get_text())
            
        rec_long_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rec_long'})
        if rec_long_post is not None and len(rec_long_post) > 0:
            player_rec_long_post = int(rec_long_post[0].get_text())
            
        rec_per_g_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rec_per_g'})
        if rec_per_g_post is not None and len(rec_per_g_post) > 0:
            player_rec_per_g_post = float(rec_per_g_post[0].get_text())
            
        rec_yds_per_g_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rec_yds_per_g'})
        if rec_yds_per_g_post is not None and len(rec_yds_per_g_post) > 0:
            player_rec_yds_per_g_post = float(rec_yds_per_g_post[0].get_text())
            
        catch_pct_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'catch_pct'})
        if catch_pct_post is not None and len(catch_pct_post) > 0:
            player_catch_pct_post = float(catch_pct_post[0].get_text())
            
        rec_yds_per_tgt_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rec_yds_per_tgt'})
        if rec_yds_per_tgt_post is not None and len(rec_yds_per_tgt_post) > 0:
            player_rec_yds_per_tgt_post = float(rec_yds_per_tgt_post[0].get_text())
            
        touches_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'touches'})
        if touches_post is not None and len(touches_post) > 0:
            player_touches_post = int(touches_post[0].get_text())
            
        yds_per_touch_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'yds_per_touch'})
        if yds_per_touch_post is not None and len(yds_per_touch_post) > 0:
            player_yds_per_touch_post = float(yds_per_touch_post[0].get_text())
            
        rush_receive_td_post = rushing_and_receiving_post.find('tfoot').find_all('td', {'data-stat': 'rush_receive_td'})
        if rush_receive_td_post is not None and len(rush_receive_td_post) > 0:
            player_rush_receive_td_post = int(rush_receive_td_post[0].get_text())

    player_games_reg_d = 0
    player_games_started_reg_d = 0
    player_def_int_reg = 0
    player_def_int_yds_reg = 0
    player_def_int_td_reg = 0
    player_def_int_long_reg = 0
    player_pass_defended_reg = 0
    player_fumbles_forced_reg = 0
    player_fumbles_reg_d = 0
    player_fumbles_rec_reg = 0
    player_fumbles_rec_yds_reg = 0
    player_fumbles_rec_td_reg = 0
    player_sacks_reg = 0
    player_tackles_combined_reg = 0
    player_tackles_solo_reg = 0
    player_tackles_assists_reg = 0
    player_tackles_loss_reg = 0
    player_qb_hits_reg = 0
    player_safety_md_reg = 0

    defense_reg = soup.find('table', {'id': 'defense'})
    if defense_reg is not None:
        
        games_reg_d = defense_reg.find('tfoot').find_all('td', {'data-stat': 'games'})
        if games_reg_d is not None and len(games_reg_d) > 0:
            player_games_reg_d = int(games_reg_d[0].get_text())
            
        games_started_reg_d = defense_reg.find('tfoot').find_all('td', {'data-stat': 'games_started'})
        if games_started_reg_d is not None and len(games_started_reg_d) > 0:
            player_games_started_reg_d = int(games_started_reg_d[0].get_text())
        
        def_int_reg = defense_reg.find('tfoot').find_all('td', {'data-stat': 'def_int'})
        if def_int_reg is not None and len(def_int_reg) > 0:
            player_def_int_reg = int(def_int_reg[0].get_text())
                
        def_int_yds_reg = defense_reg.find('tfoot').find_all('td', {'data-stat': 'def_int_yds'})
        if def_int_yds_reg is not None and len(def_int_yds_reg) > 0:
            player_def_int_yds_reg = int(def_int_yds_reg[0].get_text())

        def_int_td_reg = defense_reg.find('tfoot').find_all('td', {'data-stat': 'def_int_td'})
        if def_int_td_reg is not None and len(def_int_td_reg) > 0:
            player_def_int_td_reg = int(def_int_td_reg[0].get_text())

        def_int_long_reg = defense_reg.find('tfoot').find_all('td', {'data-stat': 'def_int_long'})
        if def_int_long_reg is not None and len(def_int_long_reg) > 0:
            player_def_int_long_reg = int(def_int_long_reg[0].get_text())
            
        pass_defended_reg = defense_reg.find('tfoot').find_all('td', {'data-stat': 'pass_defended'})
        if pass_defended_reg is not None and len(pass_defended_reg) > 0:
            player_pass_defended_reg = int(pass_defended_reg[0].get_text())
            
        fumbles_forced_reg = defense_reg.find('tfoot').find_all('td', {'data-stat': 'fumbles_forced'})
        if fumbles_forced_reg is not None and len(fumbles_forced_reg) > 0:
            player_fumbles_forced_reg = int(fumbles_forced_reg[0].get_text())
            
        fumbles_reg_d = defense_reg.find('tfoot').find_all('td', {'data-stat': 'fumbles'})
        if fumbles_reg_d is not None and len(fumbles_reg_d) > 0:
            player_fumbles_reg_d = int(fumbles_reg_d[0].get_text())

        fumbles_rec_reg = defense_reg.find('tfoot').find_all('td', {'data-stat': 'fumbles_rec'})
        if fumbles_rec_reg is not None and len(fumbles_rec_reg) > 0:
            player_fumbles_rec_reg = int(fumbles_rec_reg[0].get_text())
            
        fumbles_rec_yds_reg = defense_reg.find('tfoot').find_all('td', {'data-stat': 'fumbles_rec_yds'})
        if fumbles_rec_yds_reg is not None and len(fumbles_rec_yds_reg) > 0:
            player_fumbles_rec_yds_reg = int(fumbles_rec_yds_reg[0].get_text())

        fumbles_rec_td_reg = defense_reg.find('tfoot').find_all('td', {'data-stat': 'fumbles_rec_td'})
        if fumbles_rec_td_reg is not None and len(fumbles_rec_td_reg) > 0:
            player_fumbles_rec_td_reg = int(fumbles_rec_td_reg[0].get_text())

        sacks_reg = defense_reg.find('tfoot').find_all('td', {'data-stat': 'sacks'})
        if sacks_reg is not None and len(sacks_reg) > 0:
            player_sacks_reg = float(sacks_reg[0].get_text())
            
        tackles_combined_reg = defense_reg.find('tfoot').find_all('td', {'data-stat': 'tackles_combined'})
        if tackles_combined_reg is not None and len(tackles_combined_reg) > 0:
            player_tackles_combined_reg = int(tackles_combined_reg[0].get_text())

        tackles_solo_reg = defense_reg.find('tfoot').find_all('td', {'data-stat': 'tackles_solo'})
        if tackles_solo_reg is not None and len(tackles_solo_reg) > 0:
            player_tackles_solo_reg = int(tackles_solo_reg[0].get_text())
            
        tackles_assists_reg = defense_reg.find('tfoot').find_all('td', {'data-stat': 'tackles_assists'})
        if tackles_assists_reg is not None and len(tackles_assists_reg) > 0:
            player_tackles_assists_reg = int(tackles_assists_reg[0].get_text())
            
        tackles_loss_reg = defense_reg.find('tfoot').find_all('td', {'data-stat': 'tackles_loss'})
        if tackles_loss_reg is not None and len(tackles_loss_reg) > 0:
            player_tackles_loss_reg = int(tackles_loss_reg[0].get_text())
            
        qb_hits_reg = defense_reg.find('tfoot').find_all('td', {'data-stat': 'qb_hits'})
        if qb_hits_reg is not None and len(qb_hits_reg) > 0:
            player_qb_hits_reg = int(qb_hits_reg[0].get_text())

        safety_md_reg = defense_reg.find('tfoot').find_all('td', {'data-stat': 'safety_md'})
        if safety_md_reg is not None and len(safety_md_reg) > 0:
            player_safety_md_reg = int(safety_md_reg[0].get_text())
            
    player_games_post_d = 0
    player_games_started_post_d = 0
    player_def_int_post = 0
    player_def_int_yds_post = 0
    player_def_int_td_post = 0
    player_def_int_long_post = 0
    player_pass_defended_post = 0
    player_fumbles_forced_post = 0
    player_fumbles_post_d = 0
    player_fumbles_rec_post = 0
    player_fumbles_rec_yds_post = 0
    player_fumbles_rec_td_post = 0
    player_sacks_post = 0
    player_tackles_combined_post = 0
    player_tackles_solo_post = 0
    player_tackles_assists_post = 0
    player_tackles_loss_post = 0
    player_qb_hits_post = 0
    player_safety_md_post = 0

    defense_post = soup.find('table', {'id': 'defense_post'})
    if defense_post is not None:
        
        games_post_d = defense_post.find('tfoot').find_all('td', {'data-stat': 'games'})
        if games_post_d is not None and len(games_post_d) > 0:
            player_games_post_d = int(games_post_d[0].get_text())
            
        games_started_post_d = defense_post.find('tfoot').find_all('td', {'data-stat': 'games_started'})
        if games_started_post_d is not None and len(games_started_post_d) > 0:
            player_games_started_post_d = int(games_started_post_d[0].get_text())
        
        def_int_post = defense_post.find('tfoot').find_all('td', {'data-stat': 'def_int'})
        if def_int_post is not None and len(def_int_post) > 0:
            player_def_int_post = int(def_int_post[0].get_text())
                
        def_int_yds_post = defense_post.find('tfoot').find_all('td', {'data-stat': 'def_int_yds'})
        if def_int_yds_post is not None and len(def_int_yds_post) > 0:
            player_def_int_yds_post = int(def_int_yds_post[0].get_text())

        def_int_td_post = defense_post.find('tfoot').find_all('td', {'data-stat': 'def_int_td'})
        if def_int_td_post is not None and len(def_int_td_post) > 0:
            player_def_int_td_post = int(def_int_td_post[0].get_text())

        def_int_long_post = defense_post.find('tfoot').find_all('td', {'data-stat': 'def_int_long'})
        if def_int_long_post is not None and len(def_int_long_post) > 0:
            player_def_int_long_post = int(def_int_long_post[0].get_text())
            
        pass_defended_post = defense_post.find('tfoot').find_all('td', {'data-stat': 'pass_defended'})
        if pass_defended_post is not None and len(pass_defended_post) > 0:
            player_pass_defended_post = int(pass_defended_post[0].get_text())
            
        fumbles_forced_post = defense_post.find('tfoot').find_all('td', {'data-stat': 'fumbles_forced'})
        if fumbles_forced_post is not None and len(fumbles_forced_post) > 0:
            player_fumbles_forced_post = int(fumbles_forced_post[0].get_text())
            
        fumbles_post_d = defense_post.find('tfoot').find_all('td', {'data-stat': 'fumbles'})
        if fumbles_post_d is not None and len(fumbles_post_d) > 0:
            player_fumbles_post_d = int(fumbles_post_d[0].get_text())

        fumbles_rec_post = defense_post.find('tfoot').find_all('td', {'data-stat': 'fumbles_rec'})
        if fumbles_rec_post is not None and len(fumbles_rec_post) > 0:
            player_fumbles_rec_post = int(fumbles_rec_post[0].get_text())
            
        fumbles_rec_yds_post = defense_post.find('tfoot').find_all('td', {'data-stat': 'fumbles_rec_yds'})
        if fumbles_rec_yds_post is not None and len(fumbles_rec_yds_post) > 0:
            player_fumbles_rec_yds_post = int(fumbles_rec_yds_post[0].get_text())

        fumbles_rec_td_post = defense_post.find('tfoot').find_all('td', {'data-stat': 'fumbles_rec_td'})
        if fumbles_rec_td_post is not None and len(fumbles_rec_td_post) > 0:
            player_fumbles_rec_td_post = int(fumbles_rec_td_post[0].get_text())

        sacks_post = defense_post.find('tfoot').find_all('td', {'data-stat': 'sacks'})
        if sacks_post is not None and len(sacks_post) > 0:
            player_sacks_post = float(sacks_post[0].get_text())
            
        tackles_combined_post = defense_post.find('tfoot').find_all('td', {'data-stat': 'tackles_combined'})
        if tackles_combined_post is not None and len(tackles_combined_post) > 0:
            player_tackles_combined_post = int(tackles_combined_post[0].get_text())

        tackles_solo_post = defense_post.find('tfoot').find_all('td', {'data-stat': 'tackles_solo'})
        if tackles_solo_post is not None and len(tackles_solo_post) > 0:
            player_tackles_solo_post = int(tackles_solo_post[0].get_text())
            
        tackles_assists_post = defense_post.find('tfoot').find_all('td', {'data-stat': 'tackles_assists'})
        if tackles_assists_post is not None and len(tackles_assists_post) > 0:
            player_tackles_assists_post = int(tackles_assists_post[0].get_text())
            
        tackles_loss_post = defense_post.find('tfoot').find_all('td', {'data-stat': 'tackles_loss'})
        if tackles_loss_post is not None and len(tackles_loss_post) > 0:
            player_tackles_loss_post = int(tackles_loss_post[0].get_text())
            
        qb_hits_post = defense_post.find('tfoot').find_all('td', {'data-stat': 'qb_hits'})
        if qb_hits_post is not None and len(qb_hits_post) > 0:
            player_qb_hits_post = int(qb_hits_post[0].get_text())

        safety_md_post = defense_post.find('tfoot').find_all('td', {'data-stat': 'safety_md'})
        if safety_md_post is not None and len(safety_md_post) > 0:
            player_safety_md_post = int(safety_md_post[0].get_text())
            
    player_games_reg_ret = 0
    player_games_started_reg_ret = 0
    player_punt_ret_reg = 0
    player_punt_ret_yds_reg = 0
    player_punt_ret_td_reg = 0
    player_punt_ret_long_reg = 0
    player_punt_ret_yds_per_ret_reg = 0
    player_kick_ret_reg = 0
    player_kick_ret_yds_reg = 0
    player_kick_ret_td_reg = 0
    player_kick_ret_long_reg = 0
    player_kick_ret_yds_per_ret_reg = 0

    returns_reg = soup.find('table', {'id': 'returns'})
    if returns_reg is not None:
        
        games_reg_ret = returns_reg.find('tfoot').find_all('td', {'data-stat': 'games'})
        if games_reg_ret is not None and len(games_reg_ret) > 0:
            player_games_reg_ret = int(games_reg_ret[0].get_text())
            
        games_started_reg_ret = returns_reg.find('tfoot').find_all('td', {'data-stat': 'games_started'})
        if games_started_reg_ret is not None and len(games_started_reg_ret) > 0:
            player_games_started_reg_ret = int(games_started_reg_ret[0].get_text())
            
        punt_ret_reg = returns_reg.find('tfoot').find_all('td' , {'data-stat': 'punt_ret'})
        if punt_ret_reg is not None and len(punt_ret_reg) > 0:
            player_punt_ret_reg = int(punt_ret_reg[0].get_text())
            
        punt_ret_yds_reg = returns_reg.find('tfoot').find_all('td' , {'data-stat': 'punt_ret_yds'})
        if punt_ret_yds_reg is not None and len(punt_ret_yds_reg) > 0:
            player_punt_ret_yds_reg = int(punt_ret_yds_reg[0].get_text())
            
        punt_ret_td_reg = returns_reg.find('tfoot').find_all('td' , {'data-stat': 'punt_ret_td'})
        if punt_ret_td_reg is not None and len(punt_ret_td_reg) > 0:
            player_punt_ret_td_reg = int(punt_ret_td_reg[0].get_text())
            
        punt_ret_long_reg = returns_reg.find('tfoot').find_all('td' , {'data-stat': 'punt_ret_long'})
        if punt_ret_long_reg is not None and len(punt_ret_long_reg) > 0:
            player_punt_ret_long_reg = int(punt_ret_long_reg[0].get_text())
            
        punt_ret_yds_per_ret_reg = returns_reg.find('tfoot').find_all('td' , {'data-stat': 'punt_ret_yds_per_ret'})
        if punt_ret_yds_per_ret_reg is not None and len(punt_ret_yds_per_ret_reg) > 0:
            player_punt_ret_yds_per_ret_reg = float(punt_ret_yds_per_ret_reg[0].get_text())
            
        kick_ret_reg = returns_reg.find('tfoot').find_all('td' , {'data-stat': 'kick_ret'})
        if kick_ret_reg is not None and len(kick_ret_reg) > 0:
            player_kick_ret_reg = int(kick_ret_reg[0].get_text())
            
        kick_ret_yds_reg = returns_reg.find('tfoot').find_all('td' , {'data-stat': 'kick_ret_yds'})
        if kick_ret_yds_reg is not None and len(kick_ret_yds_reg) > 0:
            player_kick_ret_yds_reg = int(kick_ret_yds_reg[0].get_text())
            
        kick_ret_td_reg = returns_reg.find('tfoot').find_all('td' , {'data-stat': 'kick_ret_td'})
        if kick_ret_td_reg is not None and len(kick_ret_td_reg) > 0:
            player_kick_ret_td_reg = int(kick_ret_td_reg[0].get_text())
            
        kick_ret_long_reg = returns_reg.find('tfoot').find_all('td' , {'data-stat': 'kick_ret_long'})
        if kick_ret_long_reg is not None and len(kick_ret_long_reg) > 0:
            player_kick_ret_long_reg = int(kick_ret_long_reg[0].get_text())
            
        kick_ret_yds_per_ret_reg = returns_reg.find('tfoot').find_all('td' , {'data-stat': 'kick_ret_yds_per_ret'})
        if kick_ret_yds_per_ret_reg is not None and len(kick_ret_yds_per_ret_reg) > 0:
            player_kick_ret_yds_per_ret_reg = float(kick_ret_yds_per_ret_reg[0].get_text())
            
    player_games_post_ret = 0
    player_games_started_post_ret = 0
    player_punt_ret_post = 0
    player_punt_ret_yds_post = 0
    player_punt_ret_td_post = 0
    player_punt_ret_long_post = 0
    player_punt_ret_yds_per_ret_post = 0
    player_kick_ret_post = 0
    player_kick_ret_yds_post = 0
    player_kick_ret_td_post = 0
    player_kick_ret_long_post = 0
    player_kick_ret_yds_per_ret_post = 0

    returns_post = soup.find('table', {'id': 'returns_post'})
    if returns_post is not None:
        
        games_post_ret = returns_post.find('tfoot').find_all('td', {'data-stat': 'games'})
        if games_post_ret is not None and len(games_post_ret) > 0:
            player_games_post_ret = int(games_post_ret[0].get_text())
            
        games_started_post_ret = returns_post.find('tfoot').find_all('td', {'data-stat': 'games_started'})
        if games_started_post_ret is not None and len(games_started_post_ret) > 0:
            player_games_started_post_ret = int(games_started_post_ret[0].get_text())
            
        punt_ret_post = returns_post.find('tfoot').find_all('td' , {'data-stat': 'punt_ret'})
        if punt_ret_post is not None and len(punt_ret_post) > 0:
            player_punt_ret_post = int(punt_ret_post[0].get_text())
            
        punt_ret_yds_post = returns_post.find('tfoot').find_all('td' , {'data-stat': 'punt_ret_yds'})
        if punt_ret_yds_post is not None and len(punt_ret_yds_post) > 0:
            player_punt_ret_yds_post = int(punt_ret_yds_post[0].get_text())
            
        punt_ret_td_post = returns_post.find('tfoot').find_all('td' , {'data-stat': 'punt_ret_td'})
        if punt_ret_td_post is not None and len(punt_ret_td_post) > 0:
            player_punt_ret_td_post = int(punt_ret_td_post[0].get_text())
            
        punt_ret_long_post = returns_post.find('tfoot').find_all('td' , {'data-stat': 'punt_ret_long'})
        if punt_ret_long_post is not None and len(punt_ret_long_post) > 0:
            player_punt_ret_long_post = int(punt_ret_long_post[0].get_text())
            
        punt_ret_yds_per_ret_post = returns_post.find('tfoot').find_all('td' , {'data-stat': 'punt_ret_yds_per_ret'})
        if punt_ret_yds_per_ret_post is not None and len(punt_ret_yds_per_ret_post) > 0:
            player_punt_ret_yds_per_ret_post = float(punt_ret_yds_per_ret_post[0].get_text())
            
        kick_ret_post = returns_post.find('tfoot').find_all('td' , {'data-stat': 'kick_ret'})
        if kick_ret_post is not None and len(kick_ret_post) > 0:
            player_kick_ret_post = int(kick_ret_post[0].get_text())
            
        kick_ret_yds_post = returns_post.find('tfoot').find_all('td' , {'data-stat': 'kick_ret_yds'})
        if kick_ret_yds_post is not None and len(kick_ret_yds_post) > 0:
            player_kick_ret_yds_post = int(kick_ret_yds_post[0].get_text())
            
        kick_ret_td_post = returns_post.find('tfoot').find_all('td' , {'data-stat': 'kick_ret_td'})
        if kick_ret_td_post is not None and len(kick_ret_td_post) > 0:
            player_kick_ret_td_post = int(kick_ret_td_post[0].get_text())
            
        kick_ret_long_post = returns_post.find('tfoot').find_all('td' , {'data-stat': 'kick_ret_long'})
        if kick_ret_long_post is not None and len(kick_ret_long_post) > 0:
            player_kick_ret_long_post = int(kick_ret_long_post[0].get_text())
            
        kick_ret_yds_per_ret_post = returns_post.find('tfoot').find_all('td' , {'data-stat': 'kick_ret_yds_per_ret'})
        if kick_ret_yds_per_ret_post is not None and len(kick_ret_yds_per_ret_post) > 0:
            player_kick_ret_yds_per_ret_post = float(kick_ret_yds_per_ret_post[0].get_text())


    player_games_reg = max([player_games_reg_g, player_games_reg_p, player_games_reg_rr, player_games_reg_d, player_games_reg_ret])
    player_games_started_reg = max([player_games_started_reg_g, player_games_started_reg_p, player_games_started_reg_rr, player_games_started_reg_d, player_games_started_reg_ret])
    player_games_post = max([player_games_post_g, player_games_post_p, player_games_post_rr, player_games_post_d, player_games_post_ret])
    player_games_started_post = max([player_games_started_post_g, player_games_started_post_p, player_games_started_post_rr, player_games_started_post_d, player_games_started_post_ret])

    player_stats = {
        'height':height,
        'weight':weight,
        'games_reg':player_games_reg,
        'games_started_reg':player_games_started_reg,
        'games_post':player_games_post,
        'games_started_post':player_games_started_post,

        'qb_record_reg':player_qb_rec_reg,
        'pass_cmp_reg':player_pass_cmp_reg,
        'pass_att_reg':player_pass_att_reg,
        'pass_cmp_pct_reg':player_pass_cmp_pct_reg,
        'pass_yds_reg':player_pass_yds_reg,
        'pass_td_reg':player_pass_td_reg,
        'pass_td_pct_reg':player_pass_td_pct_reg,
        'pass_int_reg':player_pass_int_reg,
        'pass_int_pct_reg':player_pass_int_pct_reg,
        'pass_first_down_reg':player_pass_first_down_reg,
        'pass_success_reg':player_pass_success_reg,
        'pass_long_reg':player_pass_long_reg,
        'pass_yds_per_att_reg':player_pass_yds_per_att_reg,
        'pass_adj_yds_per_att_reg':player_pass_adj_yds_per_att_reg,
        'pass_yds_per_cmp_reg':player_pass_yds_per_cmp_reg,
        'pass_yds_per_g_reg':player_pass_yds_per_g_reg,
        'pass_rating_reg':player_pass_rating_reg,
        'pass_sacked_reg':player_pass_sacked_reg,
        'pass_sacked_yds_reg':player_pass_sacked_yds_reg,
        'pass_sacked_pct_reg':player_pass_sacked_pct_reg,
        'pass_net_yds_per_att_reg':player_pass_net_yds_per_att_reg,
        'pass_adj_net_yds_per_att_reg':player_pass_adj_net_yds_per_att_reg,
        'comebacks_reg':player_comebacks_reg,
        'gwd_reg':player_gwd_reg,
        
        'qb_record_post':player_qb_rec_post,
        'pass_cmp_post':player_pass_cmp_post,
        'pass_att_post':player_pass_att_post,
        'pass_cmp_pct_post':player_pass_cmp_pct_post,
        'pass_yds_post':player_pass_yds_post,
        'pass_td_post':player_pass_td_post,
        'pass_td_pct_post':player_pass_td_pct_post,
        'pass_int_post':player_pass_int_post,
        'pass_int_pct_post':player_pass_int_pct_post,
        'pass_first_down_post':player_pass_first_down_post,
        'pass_success_post':player_pass_success_post,
        'pass_long_post':player_pass_long_post,
        'pass_yds_per_att_post':player_pass_yds_per_att_post,
        'pass_adj_yds_per_att_post':player_pass_adj_yds_per_att_post,
        'pass_yds_per_cmp_post':player_pass_yds_per_cmp_post,
        'pass_yds_per_g_post':player_pass_yds_per_g_post,
        'pass_rating_post':player_pass_rating_post,
        'pass_sacked_post':player_pass_sacked_post,
        'pass_sacked_yds_post':player_pass_sacked_yds_post,
        'pass_sacked_pct_post':player_pass_sacked_pct_post,
        'pass_net_yds_per_att_post':player_pass_net_yds_per_att_post,
        'pass_adj_net_yds_per_att_post':player_pass_adj_net_yds_per_att_post,
        'comebacks_post':player_comebacks_post,
        'gwd_post':player_gwd_post,
        
        'rush_att_reg':player_rush_att_reg,
        'rush_yds_reg':player_rush_yds_reg,
        'rush_td_reg':player_rush_td_reg,
        'rush_first_down_reg':player_rush_first_down_reg,
        'rush_success_reg':player_rush_success_reg,
        'rush_long_reg':player_rush_long_reg,
        'rush_yds_per_att_reg':player_rush_yds_per_att_reg,
        'rush_yds_per_g_reg':player_rush_yds_per_g_reg,
        'rush_att_per_g_reg':player_rush_att_per_g_reg,
        'targets_reg':player_targets_reg,
        'rec_reg':player_rec_reg,
        'rec_yds_reg':player_rec_yds_reg,
        'rec_yds_per_rec_reg':player_rec_yds_per_rec_reg,
        'rec_td_reg':player_rec_td_reg,
        'rec_first_down_reg':player_rec_first_down_reg,
        'rec_success_reg':player_rec_success_reg,
        'rec_long_reg':player_rec_long_reg,
        'rec_per_g_reg':player_rec_per_g_reg,
        'rec_yds_per_g_reg':player_rec_yds_per_g_reg,
        'catch_pct_reg':player_catch_pct_reg,
        'rec_yds_per_tgt_reg':player_rec_yds_per_tgt_reg,
        'touches_reg':player_touches_reg,
        'yds_per_touch_reg':player_yds_per_touch_reg,
        'rush_receive_td_reg':player_rush_receive_td_reg,
        
        'rush_att_post':player_rush_att_post,
        'rush_yds_post':player_rush_yds_post,
        'rush_td_post':player_rush_td_post,
        'rush_first_down_post':player_rush_first_down_post,
        'rush_success_post':player_rush_success_post,
        'rush_long_post':player_rush_long_post,
        'rush_yds_per_att_post':player_rush_yds_per_att_post,
        'rush_yds_per_g_post':player_rush_yds_per_g_post,
        'rush_att_per_g_post':player_rush_att_per_g_post,
        'targets_post':player_targets_post,
        'rec_post':player_rec_post,
        'rec_yds_post':player_rec_yds_post,
        'rec_yds_per_rec_post':player_rec_yds_per_rec_post,
        'rec_td_post':player_rec_td_post,
        'rec_first_down_post':player_rec_first_down_post,
        'rec_success_post':player_rec_success_post,
        'rec_long_post':player_rec_long_post,
        'rec_per_g_post':player_rec_per_g_post,
        'rec_yds_per_g_post':player_rec_yds_per_g_post,
        'catch_pct_post':player_catch_pct_post,
        'rec_yds_per_tgt_post':player_rec_yds_per_tgt_post,
        'touches_post':player_touches_post,
        'yds_per_touch_post':player_yds_per_touch_post,
        'rush_receive_td_post':player_rush_receive_td_post,
        
        'def_int_reg':player_def_int_reg,
        'def_int_yds_reg':player_def_int_yds_reg,
        'def_int_td_reg':player_def_int_td_reg,
        'def_int_long_reg':player_def_int_long_reg,
        'pass_defended_reg':player_pass_defended_reg,
        'fumbles_forced_reg':player_fumbles_forced_reg,
        'fumbles_reg':player_fumbles_reg_d,
        'fumbles_rec_reg':player_fumbles_rec_reg,
        'fumbles_rec_yds_reg':player_fumbles_rec_yds_reg,
        'fumbles_rec_td_reg':player_fumbles_rec_td_reg,
        'sacks_reg':player_sacks_reg,
        'tackles_combined_reg':player_tackles_combined_reg,
        'tackles_solo_reg':player_tackles_solo_reg,
        'tackles_assists_reg':player_tackles_assists_reg,
        'tackles_loss_reg':player_tackles_loss_reg,
        'qb_hits_reg':player_qb_hits_reg,
        'safety_md_reg':player_safety_md_reg,
        
        'def_int_post':player_def_int_post,
        'def_int_yds_post':player_def_int_yds_post,
        'def_int_td_post':player_def_int_td_post,
        'def_int_long_post':player_def_int_long_post,
        'pass_defended_post':player_pass_defended_post,
        'fumbles_forced_post':player_fumbles_forced_post,
        'fumbles_post':player_fumbles_post_d,
        'fumbles_rec_post':player_fumbles_rec_post,
        'fumbles_rec_yds_post':player_fumbles_rec_yds_post,
        'fumbles_rec_td_post':player_fumbles_rec_td_post,
        'sacks_post':player_sacks_post,
        'tackles_combined_post':player_tackles_combined_post,
        'tackles_solo_post':player_tackles_solo_post,
        'tackles_assists_post':player_tackles_assists_post,
        'tackles_loss_post':player_tackles_loss_post,
        'qb_hits_post':player_qb_hits_post,
        'safety_md_post':player_safety_md_post,
        
        'punt_ret_reg':player_punt_ret_reg,
        'punt_ret_yds_reg':player_punt_ret_yds_reg,
        'punt_ret_td_reg':player_punt_ret_td_reg,
        'punt_ret_long_reg':player_punt_ret_long_reg,
        'punt_ret_yds_per_ret_reg':player_punt_ret_yds_per_ret_reg,
        'kick_ret_reg':player_kick_ret_reg,
        'kick_ret_yds_reg':player_kick_ret_yds_reg,
        'kick_ret_td_reg':player_kick_ret_td_reg,
        'kick_ret_long_reg':player_kick_ret_long_reg,
        'kick_ret_yds_per_ret_reg':player_kick_ret_yds_per_ret_reg,
        
        'punt_ret_post':player_punt_ret_post,
        'punt_ret_yds_post':player_punt_ret_yds_post,
        'punt_ret_td_post':player_punt_ret_td_post,
        'punt_ret_long_post':player_punt_ret_long_post,
        'punt_ret_yds_per_ret_post':player_punt_ret_yds_per_ret_post,
        'kick_ret_post':player_kick_ret_post,
        'kick_ret_yds_post':player_kick_ret_yds_post,
        'kick_ret_td_post':player_kick_ret_td_post,
        'kick_ret_long_post':player_kick_ret_long_post,
        'kick_ret_yds_per_ret_post':player_kick_ret_yds_per_ret_post,
    }
    
    return(player_stats)


if __name__ == '__main__':
    if os.path.exists(PLAYER_LIST_PATH) == False:    
        urls = [PLAYER_LIST_URL.format(letter) for letter in string.ascii_uppercase]
        all_data = scrape_player_lists(urls)

        player_list_df = pd.concat(all_data, ignore_index=True)
        player_list_df.insert(loc = 0, column = 'player_id', value = list(range(1, player_list_df.shape[0]+1)))
        player_list_df.to_csv(PLAYER_LIST_PATH, index=False)
    
    player_list_df = pd.read_csv(PLAYER_LIST_PATH)
    
    for i in player_list_df['player_id']:
        if player_list_df['scraped'][i-1] == False:    
            player_url = BASE_URL.format(player_list_df['link'][i-1])
            print('Scraping player #{0} - {1}'.format(i, player_list_df['name'][i-1]))
            response = scrape_page(player_url)
            if response is not None:
                player_stats = parse_player_stats_page(response)
                player_stats_dict = {'player_id':i, 
                                     'name':player_list_df['name'][i-1], 
                                     'position':player_list_df['position'][i-1],
                                     'career_begin':player_list_df['career_begin'][i-1], 
                                     'career_end':player_list_df['career_end'][i-1], 
                                     'active':player_list_df['active'][i-1]}
                player_stats_dict.update(player_stats)
                player_stats_df = pd.DataFrame(player_stats_dict, index=[0])
                
                player_stats_df.to_csv(PLAYER_STATS_PATH, mode='a', header=not os.path.exists(PLAYER_STATS_PATH), index = False)
                
                player_list_df.loc[player_list_df['player_id'] == i, 'scraped'] = True
                player_list_df.to_csv('player_list.csv', index=False)
                print('Saved data for player. Progress: {0}/{1} ({2}%)'.format(i, player_list_df.shape[0], round(i/(player_list_df.shape[0])*100, 2)))
                print()
            else:
                print("Failed to scrape page.")
            time.sleep(60/SCRAPING_RATE)