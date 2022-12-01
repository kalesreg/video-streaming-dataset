# %% [markdown]
# # Create Dataset from Wiki Tables

# %% [markdown]
# ## Import Packages

# %%
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
from datetime import datetime as dt
import statistics as stats

# %% [markdown]
# ## Functions

# %%
# read_wiki pulls in wiki data from URL and returns the soup
def read_wiki(url, params):
    req = requests.get(url, params)
    wikisoup = BeautifulSoup(req.content, "html.parser")
    return wikisoup

# get_genres parses the soup to get the list of genres
def get_genres(soup):
    genre_soup = soup.find_all(class_="mw-headline")
    genre_ls = []
    for genre in genre_soup:
        content = genre.text.strip()
        genre_ls.append(content)
    return genre_ls

# remove_genres takes out the genres that don't have tables
def remove_genres(genre_ls, remove_ls):
    for i in remove_ls:
        genre_ls.remove(i)
    return genre_ls

# get_tables parses out each genres table from the wiki soup
def get_tables(soup, genre_ls):
    tables_soup = soup.find_all("table", \
                                class_=["wikitable sortable", \
                                        "wikitable sortable plainrowheaders", \
                                        "wikitable plainrowheaders sortable"])
    tables_df = pd.DataFrame()
    for i in range(0, len(genre_ls)):
        table = tables_soup[i]
        df = pd.read_html(str(table))
        df = pd.DataFrame(df[0])
        tables_df = pd.concat([tables_df, df], ignore_index=True, sort=False)
    return tables_df

# %% [markdown]
# ## Data Setup

# %%
# Request requirements for each url
PARAMS = {"client":"firefox-b-d", "User-agent":"kalesreg"}
nflx_cont_url = "https://en.wikipedia.org/wiki/List_of_Netflix_original_programming"
nflx_end_url = "https://en.wikipedia.org/wiki/List_of_ended_Netflix_original_programming"
hulu_url = "https://en.wikipedia.org/wiki/List_of_Hulu_original_programming"
amzn_url = "https://en.wikipedia.org/wiki/List_of_Amazon_Prime_Video_original_programming"
apl_url = "https://en.wikipedia.org/wiki/List_of_Apple_TV%2B_original_programming"
prmt_url = "https://en.wikipedia.org/wiki/List_of_Paramount%2B_original_programming"
dis_url = "https://en.wikipedia.org/wiki/List_of_Disney%2B_original_programming"
hbo_url = "https://en.wikipedia.org/wiki/List_of_HBO_Max_original_programming"
#url_ls = [nflx_cont_url, nflx_end_url, hulu_url, amzn_url, apl_url, prmt_url, dis_url, hbo_url]

# %% [markdown]
# ## Netflix Data

# %%
# Netflix Current Data
nflx_cont_soup = read_wiki(nflx_cont_url, PARAMS)
nflx_cont_genres = get_genres(nflx_cont_soup)
#print(nflx_cont_genres)
# Will need to update end based on upcoming genre or stop point 
# based on changes to wiki page
end = nflx_cont_genres.index("Upcoming original programming")
nflx_cont_genres = nflx_cont_genres[:end]
# Will need to update remove list for headers that don't have tables 
# based on changes to wiki page
nflx_cont_remove = ["Animation", "Non-English language scripted", "Unscripted", "Specials"]
nflx_cont_genres = remove_genres(nflx_cont_genres, nflx_cont_remove)
#print(nflx_cont_genres)
nflx_cont_df = get_tables(nflx_cont_soup, nflx_cont_genres)
#print(nflx_cont_df)
nflx_cont_df["Cur. service"] = "Netflix"
nflx_cont_df = nflx_cont_df[["Cur. service", "Title", "Genre", "Premiere", "Seasons", "Status", "Language", "Runtime"]]
print(nflx_cont_df)

# Netflix Ended Data
nflx_end_soup = read_wiki(nflx_end_url, PARAMS)
nflx_end_genres = get_genres(nflx_end_soup)
#print(nflx_end_genres)
# Will need to update end based on upcoming genre or stop point 
# based on changes to wiki page
end = nflx_end_genres.index("Notes")
nflx_end_genres = nflx_end_genres[:end]
# Will need to update remove list for headers that don't have tables 
# based on changes to wiki page
nflx_end_remove = ["Animation", "Non-English language scripted", "Unscripted", "Specials", "Regional original programming", "Animation", "Non-English language scripted", "Unscripted"]
nflx_end_genres = remove_genres(nflx_end_genres, nflx_end_remove)
#print(nflx_end_genres)
nflx_end_df = get_tables(nflx_end_soup, nflx_end_genres)
#print(nflx_end_df)
for x in range(0, len(nflx_end_df)):
    if pd.isnull(nflx_end_df.loc[x, "Premiere"]):
        nflx_end_df.loc[x, "Premiere"] = nflx_end_df.loc[x, "Release date"]
nflx_end_df["Seasons"] = nflx_end_df[['Seasons', 'Episodes']].stack().groupby(level=0).agg(' '.join)
nflx_end_df["Status"] = "Ended"
nflx_end_df["Cur. service"] = "Netflix"
nflx_end_df = nflx_end_df[["Cur. service", "Title", "Genre", "Premiere", "Seasons", "Status", "Language", "Runtime"]]
print(nflx_end_df)

# %% [markdown]
# ## Hulu Data

# %%
# Hulu Data
hulu_soup = read_wiki(hulu_url, PARAMS)
hulu_genres = get_genres(hulu_soup)
#print(hulu_genres)
# Will need to update end based on upcoming genre or stop point 
# based on changes to wiki page
end = hulu_genres.index("Upcoming original programming")
#print(end)
hulu_genres = hulu_genres[:end]
# Will need to update remove list for headers that don't have tables 
# based on changes to wiki page
hulu_remove = ["Original programming", "Animation", "Unscripted", "Hotstar"]
hulu_genres = remove_genres(hulu_genres, hulu_remove)
#print(hulu_genres)
hulu_df = get_tables(hulu_soup, hulu_genres)
#print(hulu_df)
hulu_df.rename(columns={"Length":"Runtime"}, inplace=True)
hulu_df["Cur. service"] = "Hulu"
hulu_df = hulu_df[["Cur. service", "Title", "Genre", "Premiere", "Seasons", "Status", "Language", "Runtime", "Prev. network(s)"]]
print(hulu_df)

# %% [markdown]
# ## Amazon Prime Data

# %%
# Amazon Prime Data
amzn_soup = read_wiki(amzn_url, PARAMS)
amzn_genres = get_genres(amzn_soup)
#print(amzn_genres)
# Will need to update end based on upcoming genre or stop point 
# based on changes to wiki page
end = amzn_genres.index("Upcoming original programming")
amzn_genres = amzn_genres[:end]
# Will need to update remove list for headers that don't have tables 
# based on changes to wiki page
amzn_remove = ["Original programming", "Animation", "Non-English language scripted", "Unscripted", "Regional original programming"]
amzn_genres = remove_genres(amzn_genres, amzn_remove)
#print(amzn_genres)
amzn_df = get_tables(amzn_soup, amzn_genres)
#print(amzn_df)
amzn_df = amzn_df.rename(columns={"Seasons/episodes":"Seasons", "Previous channel":"Prev. network(s)"})
amzn_df["Runtime"] = "na"
amzn_df["Cur. service"] = "Amazon Prime"
amzn_df = amzn_df[["Cur. service", "Title", "Genre", "Premiere", "Seasons", "Status", "Language", "Runtime", "Prev. network(s)"]]
print(amzn_df)

# %% [markdown]
# ## Apple TV+ Data

# %%
# Apple TV+ Data
apl_soup = read_wiki(apl_url, PARAMS)
apl_genres = get_genres(apl_soup)
#print(apl_genres)
# Will need to update end based on upcoming genre or stop point 
# based on changes to wiki page
end = apl_genres.index("Upcoming original programming")
apl_genres = apl_genres[:end]
# Will need to update remove list for headers that don't have tables 
# based on changes to wiki page
apl_remove = ["Original programming", "Animation", "Unscripted"]
apl_genres = remove_genres(apl_genres, apl_remove)
#print(apl_genres)
apl_df = get_tables(apl_soup, apl_genres)
#print(apl_df)
apl_df["Language"] = np.NaN
apl_df["Prev. network(s)"] = np.NaN
apl_df["Cur. service"] = "Apple TV+"
apl_df = apl_df[["Cur. service", "Title", "Genre", "Premiere", "Seasons", "Status", "Language", "Runtime", "Prev. network(s)"]]
print(apl_df)

# %% [markdown]
# ## Paramount+ Data

# %%
# Paramount+ Data
prmt_soup = read_wiki(prmt_url, PARAMS)
prmt_genres = get_genres(prmt_soup)
#print(prmt_genres)
# Will need to update end based on upcoming genre or stop point 
# based on changes to wiki page
end = prmt_genres.index("Upcoming original programming")
prmt_genres = prmt_genres[:end]
# Will need to update remove list for headers that don't have tables 
# based on changes to wiki page
prmt_remove = ["Original programming", "Animation", "Unscripted", "Regional original programming", "Non-English language"]
prmt_genres = remove_genres(prmt_genres, prmt_remove)
#print(prmt_genres)
prmt_df = get_tables(prmt_soup, prmt_genres)
#print(prmt_df)
prmt_df["Cur. service"] = "Paramount+"
prmt_df = prmt_df[["Cur. service", "Title", "Genre", "Premiere", "Seasons", "Status", "Language", "Runtime", "Prev. network(s)"]]
print(prmt_df)

# %% [markdown]
# ## Disney+ Data

# %%
# Disney+ Data
dis_soup = read_wiki(dis_url, PARAMS)
#print(dis_soup)
dis_genres = get_genres(dis_soup)
#print(dis_genres)
# Will need to update end based on upcoming genre or stop point 
# based on changes to wiki page
end = dis_genres.index("Upcoming original programming")
dis_genres = dis_genres[:end]
# Will need to update remove list for headers that don't have tables 
# based on changes to wiki page
dis_remove = ["Original programming", "Non-English language", "Unscripted", "Specials"]
dis_genres = remove_genres(dis_genres, dis_remove)
#print(dis_genres)
dis_df = get_tables(dis_soup, dis_genres)
#print(dis_df)
dis_df["Language"] = np.NaN
dis_df["Cur. service"] = "Disney+"
dis_df = dis_df[["Cur. service", "Title", "Genre", "Premiere", "Seasons", "Status", "Language", "Runtime"]]
print(dis_df)

# %% [markdown]
# ## HBO Max Data

# %%
# HBO Max Data
hbo_soup = read_wiki(hbo_url, PARAMS)
#print(hbo_soup)
hbo_genres = get_genres(hbo_soup)
#print(hbo_genres)
# Will need to update end based on upcoming genre or stop point 
# based on changes to wiki page
end = hbo_genres.index("Original podcasts")
hbo_genres = hbo_genres[:end]
# Will need to update remove list for headers that don't have tables 
# based on changes to wiki page
hbo_remove = ["Original programming", "Animation", "Non-English language scripted", "Unscripted"]
hbo_genres = remove_genres(hbo_genres, hbo_remove)
#print(hbo_genres)
hbo_df = get_tables(hbo_soup, hbo_genres)
#print(hbo_df)
hbo_df["Cur. service"] = "HBO Max"
hbo_df = hbo_df[["Cur. service", "Title", "Genre", "Premiere", "Seasons", "Status", "Language", "Runtime", "Prev. network(s)"]]
print(hbo_df)

# %% [markdown]
# ## Combine Data

# %%
# Combine all the dataframes into one dataframe
df_ls = [nflx_cont_df, nflx_end_df, hulu_df, amzn_df, apl_df, prmt_df, dis_df, hbo_df]
all_df = pd.concat(df_ls, axis=0)
print(all_df)

# %% [markdown]
# ## Clean Data

# %%
# Clean all wikipedia data

# Misc fixes
# Remove rows that are empty based on table formatting on the wiki pages
all_df = all_df[all_df["Title"].notnull()]
all_df = all_df[(all_df["Title"] != "Awaiting release") & (all_df["Genre"] != "Awaiting release")]
# Split seasons and episodes into two columns
all_df[['Seasons', 'Episodes']] = all_df['Seasons'].str.split(', ', 1, expand=True)
# Reset index after combining dataframes and removing rows
all_df = all_df.reset_index(drop=True)

for entry in range(0, len(all_df)):
    # Remove citations from Title
    if all_df["Title"][entry] is not np.NaN:
        all_df["Title"][entry] = re.sub("[\[\(].*?[\)\]]", "", all_df["Title"][entry]).rstrip()
    
    # Remove citations from Premiere and convert to date data type
    all_df["Premiere"][entry] = re.sub("[\[\(].*", "", str(all_df["Premiere"][entry])).rstrip()
    if str(all_df["Premiere"][entry]) not in ["nan", "TBA", "2022", "2023", "2024", "Summer 2022", "Mid 2022", "Late 2022"]:
        if bool(re.search("-", all_df["Premiere"][entry])):
            all_df["Premiere"][entry] = dt.strptime(all_df["Premiere"][entry], "%Y-%m-%d %H:%M:%S")
        else:
            all_df["Premiere"][entry] = dt.strptime(all_df["Premiere"][entry], "%B %d, %Y")
    else:
        all_df["Premiere"][entry] = "nan"
    
    # Clean Seasons and Episodes
    # Put miniseries episodes in episodes and mark seasons as 1
    if "episode" in str(all_df["Seasons"][entry]):
        all_df["Episodes"][entry] = all_df["Seasons"][entry]
        all_df["Seasons"][entry] = "1 season"
    # Remove episode label so data is just a number
    all_df["Episodes"][entry] = str(all_df["Episodes"][entry]).split(" ")[0]
    # Mark episodes as unknown
    if all_df["Episodes"][entry] in ["nan", "None", "TBA"]:
        all_df["Episodes"][entry] = "Unknown"
    # Remove season label so data is just a number
    all_df["Seasons"][entry] = str(all_df["Seasons"][entry]).split(" ")[0]
    # Mark seasons as unknown
    if all_df["Seasons"][entry] in ["nan", "TBA"]:
        all_df["Seasons"][entry] = "Unknown"

    # Remove citations from Status and clean data labels
    if all_df["Status"][entry] is not np.NaN:
        no_note = re.sub("\[.*?\]", "", all_df["Status"][entry])
        rename = ["due to premiere", "ongoing", "renewed", "release", "pre-production", "series order", "post-production", "filming"]
        for item in rename:
            if item in no_note.lower():
                replace = "Continuing"
                break
            else:
                replace = no_note
        all_df["Status"][entry] = replace
        
    # Change runtime into buckets (less than 30 min, more than 60, or between 30 and 60) based on median value of range 
    all_df["Runtime"][entry] = re.sub("[\[\(].*?[\)\]]", "", all_df["Runtime"][entry]).rstrip()
    all_df["Runtime"][entry] = re.sub("~", "", all_df["Runtime"][entry]).rstrip()
    if "h" in str(all_df["Runtime"][entry]):
        all_df["Runtime"][entry] = ">60 min"
    else:
        temp = re.sub("[min].*", "", all_df["Runtime"][entry]).rstrip()
        if temp in ["TBA", "na", ""]:
            all_df["Runtime"][entry] = "Unknown"
        else:
            nums = re.split("[\D]", temp)
            nums = [float(x) for x in nums]
            mid = stats.median(nums)
            if mid < 30:
                all_df["Runtime"][entry] = "<30 min"
            elif mid > 60:
                all_df["Runtime"][entry] = ">60 min"
            else:
                all_df["Runtime"][entry] = "<60 min"

# %% [markdown]
# ## Tidy Data

# %%
all_df

# %% [markdown]
# ## Save Data to CSV

# %%
all_df.to_csv("original_tv_streaming.csv")


