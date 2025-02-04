# Pro Football Reference Scraper
This repo contains the code for a web scraper I wrote for [pro-football-reference.com](https://www.pro-football-reference.com/). It scrapes the career statistics for all NFL players, both retired and active. I built this mostly for personal use as I wanted a comprehensive, simple dataset that encompassed all NFL players and their statistics. The resulting dataset can be used for a variety of historical data analysis, interesting visualizations, or predictive modeling. 

# Data
The web scraper outputs 2 files to the `/data/` directory, `player_list.csv` and `player_stats.csv`.

## player_list.csv
There are 8 fields in 'player_list.csv'. This can be used as a list of all NFL players from history if you are not interested in the player's career statistics. However, it is mostly used by the scraper to save its progress while scraping each player's stats to ensure it does not repeat any players if interrupted.

- player_id: The assigned ID for the player.
- link: This is the extension of the player's personal PFR page. Appending this string to 'https://www.pro-football-reference.com/' results in the link to the player's PFR page.
- name: The player's full name.
- position: The player's position. In the case of players with multiple positions, there are contained in a single string, separated by hyphens, e.g. "QB-WR-RB".
- career_begin: The year the player began their career.
- career_end: The year the player ended their career.
- active: A boolean indicating whether the player is currently active in the NFL or not.
- scraped: A boolean indicating whether the player has already been scraped. When the scraper is first run, all these values will be set to False. As each player's statistics are scraped, their value will be updated to True. This is to ensure the scraper does not repeat players it has already scraped if it gets interrupted while running.

## player_stats.csv
There are 162 fields in 'player_stats.csv'. This is the main dataset output by the web scraper. It contains all player's career statistics across all of NFL history. It contains the same player information as 'player_list.csv', with the addition of height and weight, plus the player's career statistics. These statistics can be grouped into 5 categories: games played statistics, passing statistics, rushing & receiving statistics, defensive & fumble statistics, and punt/kick return statistics. 

### Player information
- player_id: The assigned ID for the player.
- name: The player's full name.
- position: The player's position. In the case of players with multiple positions, there are contained in a single string, separated by hyphens, e.g. "QB-WR-RB".
- career_begin: The year the player began their career.
- career_end: The year the player ended their career.
- active: A boolean indicating whether the player is currently active in the NFL or not.
- height: The player's height in the format "feet-inches", stored as a string. 
- weight: The player's weight in pounds, stored as an int.

For each of the following statistics, there are two columns, one with the suffix <code>_reg</code> and one with the suffix <code>_post</code> to indicate the player's career regular season numbers and career postseason numbers. See the games played statistics below as an example.

### Games Played Statistics
- games_reg: Number of regular season games played.
- games_started_reg: Number of regular season games started.
- games_post: Number of postseason games played.
- games_started_post: Number of postseason games started.

Going forward, for all other statistics, there will only be one entry in this data dictionary for both the regular and postseason statistics in the data. As seen with the game statistics above, the naming convention is that regular season numbers have the suffix <code>_reg</code> appended to the column name in this data dictionary, while postseason numbers have the suffix <code>_post</code> appended.

### Passing Statistics
- qb_record: Team record in games started by this QB.
- pass_cmp: Passes completed.
- pass_att: Passes attempted.
- pass_cmp_pct: Percentage of passes completed.
- pass_yds: Yards gained by passing.
- pass_td: Passing touchdowns.
- pass_td_pct: Percentages of touchdowns thrown when attempting to pass.
- pass_int: Interceptions thrown.
- pass_int_pct: Percentages of times intercepted when attempting to pass.
- pass_first_down: First downs passing.
- pass_success: Passing success rate. A successful pass gains at least 40% of yards required on 1st down, 60% of yards required on 2nd down, and 100% on 3rd and 4th down. Denominator is pass attempts + times sacked.
- pass_long: Longest completed pass thrown.
- pass_yds_per_att: Yards gained per pass attempt.
- pass_adj_yds_per_att: Adjusted yards gained per pass attempt. The formula is (Passing yards + 20 \* Passing TD - 45 \* Interceptions)/(Passes Attempted).
- pass_yds_per_cmp: Yards gained per pass completion.
- pass_yds_per_g: Yards gained per game played.
- pass_rating: NFL passer rating. See page [here](https://en.wikipedia.org/wiki/Passer_rating) for the formula.
- pass_sacked: Times sacked (first recorded in 1969, player per game since 1981).
- pass_sacked_yds: Yards lost due to sacks (first recorded in 1969, player per game since 1981).
- pass_sacked_pct: Percentage of times sacked when attempting to pass. The formula is Times sacked / (Passes attempted + Times sacked).
- pass_net_yds_per_att: Net yards gained per pass attempt. The formula is (Passing Yards - Sack Yards) / (Passes Attempted + Times Sacked).
- pass_adj_net_yds_per_att: Adjusted net yards per pass attempt. The formula is (Passing Yards - Sack Yards + (20 \* Passing TD) - (45 \* Interceptions)) / (Passes Attempted + Times Sacked).
- comebacks: Comebacks led by quarterback.
- gwd: Game-winning drives led by quarterback.

### Rushing & Receiving Statistics
- rush_att: Rushing attempts (sacks not included in NFL).
- rush_yds: Rushing yards gained (sack yardage is not included by NFL).
- rush_td: Rushing touchdowns.
- rush_first_down: First downs rushing.
- rush_success: Rushing success rate. A successful rush gains least 40% of yards required on 1st down, 60% of yards required on 2nd down, and 100% on 3rd or 4th down. Denominator is rushing attempts.
- rush_long: Longest rushing attempt.
- rush_yds_per_att: Rushing yards per attempt.
- rush_yds_per_g: Rushing yards per game.
- rush_att_per_g: Rushing attempts per game.
- targets: Pass targets (since 1992, derived from NFL play-by-play data).
- rec: Receptions.
- rec_yds: Receiving yards.
- rec_yds_per_rec: Receiving yards per reception.
- rec_td: Receiving touchdowns.
- rec_first_down: First downs receiving.
- rec_success: Receiving success rate. A successful reception gains at least 40% of yards required on 1st down, 60% of yards required on 2nd down, and 100% on 3rd or 4th down. Denominator is targets.
- rec_long: Longest reception.
- rec_per_g: Receptions per game
- rec_yds_per_g: Receiving yards per game.
- catch_pct: Catch percentage. The formula is receptions / targets. (since 1992).
- rec_yds_per_tgt: Receiving yards per target (target numbers since 1992).
- touches: Rushing attempts and receptions.
- yds_per_touch: Scrimmage yards per touch, rushing + receiving yardage per opportunity.
- rush_receive_td: Rushing and receiving touchdowns.

### Defense & Fumble Statistics
- def_int: Passes intercepted on defense.
- def_int_yds: Yards interceptions were returned.
- def_int_td: Interceptions returned for touchdowns.
- def_int_long: Longest interception return.
- pass_defended: Passes defended, since 1999.
- fumbles_forced: Number of times player forced a fumble recovered by either team.
- fumbles: Number of times player fumbled, both lost and recovered by own team. This represents ALL fumbles by the player on offense, defense, and special teams.
- fumbles_rec: Fumbles recovered by a player or team. Original fumble by either team.
- fumbles_rec_yds: Yards recovered fumbles were returned.
- fumbles_rec_td: Fumbles recovered reuslting in a touchdown for the recoverer.
- sacks: Sacks. (Official since 1982, based on play-by-play, game film, and other research since 1960).
- tackles_combined: Combined solo + assisted tackles. Prior to 1994, all tackles are put into 'combined'.
- tackles_solo: Solo tackles. Prior to 1994, unofficial and inconsistently recorded from team to team. After 1994, unofficial but consistently recorded.
- tackles_assists: Assists on tackles. Prior of 1994, combined with solo tackles. After 1994, unofficial but consistently recorded.
- tackles_loss: Tackles for loss, recorded for 95% of games from 1999-2007 and 100% since 2008.
- qb_hits: Quarterback hits, recorded since 2006.
- safety_md: Safeties scored by player.

### Punt/Kick Return Statistics
- punt_ret_reg: Punts returned.
- punt_ret_yds_reg: Punts return yardage.
- punt_ret_td_reg: Punts returned for touchdown.
- punt_ret_long_reg: Longest punt return.
- punt_ret_yds_per_ret_reg: Yards per punt return.
- kick_ret_reg: Kickoffs returned.
- kick_ret_yds_reg: Yardage for kickoffs returned.
- kick_ret_td_reg: Kickoffs returned for a touchdown.
- kick_ret_long_reg: Longest kickoff return.
- kick_ret_yds_per_ret_reg: Yards per kickoff return.

# To-Do
- [x] Write data dictionary for scraper
- [ ] Separate height and weight regex into 2 separate regex
- [ ] Add kicking/punting stats

