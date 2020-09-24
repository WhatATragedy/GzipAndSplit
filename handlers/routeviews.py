from bs4 import BeautifulSoup
import pandas
import requests
import datetime
import os
import errno
import bz2
import glob
import subprocess

class RouteViews():
    def __init__(self):
        self._peering_status = 'http://www.routeviews.org/peers/peering-status.html'
        self._collectors = self._get_routeview_collectors()
        self._rib_endpoint = 'http://archive.routeviews.org/'
        #http://archive.routeviews.org/route-views.amsix/bgpdata/
        self._MAX_AGE = 90
        self.collectedFiles = []
        return None
    
    @staticmethod
    def _get_routeview_collectors():
        r = requests.get('http://www.routeviews.org/routeviews/index.php/collectors/')
        rv_collector_df = pandas.read_html(r.text)[0].dropna(axis=0, thresh=4)
        results  = rv_collector_df.loc[rv_collector_df['UI'] == 'telnet', 'Host'].tolist()
        results = [result.replace('.routeviews.org', '') for result in results]
        return results
        
    def _get_routeview_devices(self):
        r = requests.get('http://archive.routeviews.org/')
        soup = BeautifulSoup(r.text, 'html.parser')
        results = soup.find('li', string='Data Archives')
        print (results)

    @staticmethod
    def create_intervals(limit=2400):
        intervals = []
        for interval in range(0,limit, 100):
            interval = str(interval).rjust(4, '0')
            intervals.append(interval)
        return intervals


    def getRibFile(self, outputDirectory, latestFileURL, collector):
        if not self.isFileTooOld(latestFileURL.split("/")[-1]):
            try:
                os.mkdir(f'{outputDirectory}/{collector}')
            except OSError as e:
                if e.errno == errno.EEXIST:
                    pass

            for filename in os.listdir(f'{outputDirectory}/{collector}'):
                os.remove(f'{outputDirectory}/{collector}/{filename}')
            
            print(f'Getting Rib - {collector}')
            r = requests.get(latestFileURL)
            print(f'Completed Collection of {collector}')
            try:
                print(f"Beginning Decompresion of {collector}...")
                decompress_data = bz2.decompress(r.content)
                open(f'{outputDirectory}/{collector}.decomp', 'wb').write(decompress_data)

            except Exception as e:
                print(f'Error Unzipping File Contents for {collector}: {e}')
                return None
            print(f"Finished {collector}...")
            return f"{outputDirectory}/{collector}.decomp"

        
    def get_ribs(self, outputDirectory, collector_list=None):
        #http://archive.routeviews.org/route-views.amsix/bgpdata/2020.06/RIBS/rib.20200601.0000.bz2
        collector_list = collector_list if collector_list is not None else self._collectors
        try:
            os.mkdir(outputDirectory)
        except OSError as e:
            if e.errno == errno.EEXIST:
                pass
        files = []
        for collector in collector_list:
            #check if the collector has data in recent months
            latestFileURL = self.scrapeLatestDate(collector)
            if latestFileURL == None:
                print(f"Issue with {collector}, skipping...")
                continue
            ##TODO add a func to check if the latest date is not too old
            file = self.getRibFile(outputDirectory, latestFileURL, collector)
            if file is not None:
                self.collectedFiles.append(file)
            

    @staticmethod
    def current_rib_files(ribs_directory=None):
        ribs_directory = ribs_directory if ribs_directory is not None else 'ribs'
        files = []
        for directory in os.listdir(ribs_directory):
            for filename in os.listdir(f"{ribs_directory}/{directory}"):
                files.append(f"{ribs_directory}{directory}/{filename}")
        return files

    def scrapeLatestDate(self, collector):
        #latest month directory http://archive.routeviews.org/route-views.amsix/bgpdata/
        #latest file directory http://archive.routeviews.org/route-views.amsix/bgpdata/2020.09/RIBS/rib.20200901.0000.bz2
        monthURL = f"{self._rib_endpoint}{collector}/bgpdata/"
        r = requests.get(monthURL)
        soup = BeautifulSoup(r.text, 'html.parser')
        aTags = soup.find_all('a')
        #get the last item as that will be the most recent files
        if len(aTags) == 0:
            return None
        mostRecent = aTags[-1].text ##maybe I should check if it's actually a month but nah

        #got most recent month now scrape the files for most recent file
        fileDirURL = f"{self._rib_endpoint}{collector}/bgpdata/{mostRecent}/RIBS/"
        r = requests.get(fileDirURL)
        soup = BeautifulSoup(r.text, 'html.parser')
        aTags = soup.find_all('a')
        if len(aTags) == 0:
            return None
        mostRecentFile = aTags[-1].text
        try:
            mostRecentFileDatetime = mostRecentFile.replace("rib.", "").replace(".bz2", "")
            mostRecentFileDatetime= datetime.datetime.strptime(mostRecentFileDatetime, '%Y%m%d.%H%M')
            return f"{self._rib_endpoint}{collector}/bgpdata/{mostRecent}/RIBS/{mostRecentFile}"
        except ValueError:
            return None

    def isFileTooOld(self, ribFile, maxAge=None):
        maxAge = maxAge if maxAge is not None else self._MAX_AGE
        fileDatetime = datetime.datetime.strptime(ribFile.replace("rib.", "").replace(".bz2", ""), '%Y%m%d.%H%M')
        if fileDatetime < datetime.datetime.now() - datetime.timedelta(days = maxAge):
            return True
        else:
            return False
    def parseFiles(self, inputDirectory=None):
        if len(self.collectedFiles) == 0:
            ##parse directory instead
            filesToParse = []
            for filename in glob.glob(f"{inputDirectory}/*"):
                filesToParse.append(os.path.abspath(filename))
        else:
            filesToParse = self.collectedFiles
        for filename in filesToParse:
            print(filename)
            self.runCommand(filename)

    @staticmethod
    def runCommand(filename):
        outputFilename = filename.replace("decomp","hmnread")
        with open(f"{outputFilename}", "wb") as outputFile:
            print(f"gc {filename}| select -first 10")
            process = subprocess.Popen(f"gc {filename}| select -first 10", stdout=subprocess.PIPE, shell=True)
            while True:
                output = process.stdout.readline()
                print(output)
                if (output == '' or output == b'') and process.poll() is not None:
                    break
                if output:
                    outputFile.write(output.replace("|", ","))
            rc = process.poll()
            return rc


        

        
        
        




