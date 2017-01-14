import database_operations as dbo
import XMLStats
import pandas as pd


class XMLStats():
    def __init__(self, sport, start_date, end_date):
        """
        sport - 'NBA' or 'MLB'
        start_date/end_date - 'YYYY-MM-DD'
        historize - TRUE or FALSE
        """

        self.sport = sport
        self.start_date = start_date
        self.end_date = end_date
        self.historize = historize
        self.conn = dbo.db_connect()

    def get_events(self,date):
        data = XMLStats.main(self.sport,'events',{'sport':self.sport,'date':date})
        if data['event']:
            event_ids = [event['event_id'] for event in data['event']]
            return event_ids
        return

    def boxscore(self,gameid):
        game_data = XMLStats.main(self.sport.lower(),'boxscore',None,gameid)
        if game_data:
            self.parse_event_data(gameid,game_data)
            self.parse_player_data(gameid,game_data)
        return

    def main(self):
        for date in [d.strftime('%Y-%m-%d') for d in pd.date_range(self.start_date, self.end_date)]:
            event_ids = self.get_events(date)
            for gameid in event_ids:
                self.boxscore(gameid)
        return



class NBA(XMLStats):
    def __init__(self, sport, start_date, end_date):
        """
        sport - 'NBA' or 'MLB'
        start_date/end_date - 'YYYY-MM-DD'
        historize - TRUE or FALSE
        """
        XMLStats.__init__(self, sport, start_date, end_date)
        self.team_stats = ['three_point_field_goals_attempted','three_point_field_goals_made',
                            'field_goals_attempted','field_goals_made','free_throws_attempted',
                            'free_throws_made','assists','blocks','personal_fouls','offensive_rebounds',
                            'defensive_rebounds','steals','turnovers']

        self.player_stats = ['three_point_field_goals_attempted','three_point_field_goals_made',
                            'field_goals_attempted','field_goals_made','free_throws_attempted',
                            'free_throws_made','assists','blocks','personal_fouls','minutes','points',
                            'offensive_rebounds','defensive_rebounds','steals','turnovers']

    def parse_player_data(self,gameid,game_data):
        opp_dict = {game_data['away_team']["abbreviation"]:game_data['home_team']["abbreviation"],
                    game_data['home_team']["abbreviation"]:game_data['away_team']["abbreviation"]}

        start_time = datetime.datetime.strptime(game_data['start_date_time'][:-6],'%Y-%m-%dT%H:%M:%S')


        away_data = [(gameid,p['team_abbreviation'],opp_dict[p['team_abbreviation']],p['position'],
                        p['display_name']) + tuple([p[stat] for stat in self.player_stats]) + \
                        ((1 if p['is_starter'] else 0),start_time.date()) for p in game_data['away_stats']]

        home_data = [(gameid,p['team_abbreviation'],opp_dict[p['team_abbreviation']],p['position'],
                        p['display_name']) + tuple([p[stat] for stat in self.player_stats]) + \
                        ((1 if p['is_starter'] else 0),start_time.date()) for p in game_data['home_stats']]

        query = "DELETE FROM player_data WHERE date=%s"
        dbo.execute_query(self.conn,query,(start_time.date(),))

        query = "INSERT INTO player_data VALUES (" + '%s,'*21 + "%s)"
        dbo.execute_query(self.conn,query,away_stats+home_stats,True)
        return


    def parse_event_data(self,gameid,game_data):
        officials = [o['first_name'] + ' ' + o['last_name'] for o in game_data['officials']]
        if len(officials)<4:
            officials += None

        duration = int(game_data['duration'][0])*60 + int(game_data['duration'][2:4])

        away_stats = [game_data['away_totals'][stat] for stat in self.team_stats]
        away_scores = game_data['away_period_scores'] + [None,None,None,None]

        home_stats = [game_data['home_totals'][stat] for stat in self.team_stats]
        home_scores = game_data['home_period_scores'] + [None,None,None,None]

        start_time = datetime.datetime.strptime(game_data['start_date_time'][:-6],'%Y-%m-%dT%H:%M:%S')

        data = (gameid,game_data['home_team']['abbreviation'],game_data['away_team']['abbreviation'],
                game_data['attendance']) + tuple(officials) + (game_data['season_type'],
                sum(game_data['home_period_scores']),sum(game_data['away_period_scores']),duration) + \
                tuple(away_stats) + tuple(away_scores[0:8]) + tuple(home_stats) + tuple(home_scores[0:8]) + \
                (start_time.date(),start_time)

        query = "DELETE FROM event_data WHERE date=%s"
        dbo.execute_query(self.conn,query,(start_time.date(),))

        query = "INSERT INTO event_data VALUES(" + "%s,"*56 + "%s)"
        dbo.execute_query(self.conn,query,data,False)
        return


class MLB(XMLStats):
    def __init__(self, sport, start_date, end_date, historize):
        """
        sport - 'NBA' or 'MLB'
        start_date/end_date - 'YYYY-MM-DD'
        historize - TRUE or FALSE
        """
        XMLStats.__init__(self, sport, start_date, end_date, historize)
        self.team_stats = []

        self.player_stats = []

    def parse_player_data(self,gameid,game_data):


        query = "INSERT INTO player_data VALUES (" + '%s,'*21 + "%s)"
        dbo.execute_query(self.conn,query,away_stats+home_stats,True)
        return


    def parse_event_data(self,gameid,game_data):

        query = "INSERT INTO event_data VALUES(" + "%s,"*56 + "%s)"
        dbo.execute_query(self.conn,query,data,False)
        return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='XMLStats Data Retrieval and Storage')
    parser.add_argument('sport', default='NBA', help='Sport to run get data for -- NBA or MLB')
    parser.add_argument('--start_date', default='2012-01-01', help='Starting date for data')
    parser.add_argument('--end_date', default='2012-01-01', help='Ending date for data')

    if args.sport =='NBA':
        sport = NBA('NBA',args.start_date,args.end_date)
    else:
        sport = MLB('MLB',args.start_date,args.end_date)

    sport.main()
