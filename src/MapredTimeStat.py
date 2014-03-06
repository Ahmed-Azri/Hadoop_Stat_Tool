import datetime
import sys

class MapredTimeStat:
	def __init__(self, localSide, filenameList):
		
	def computeTimeStat(self):
	def dumpResultToFile(self, outputFile):

def main():
	if len(sys.argv) < 3:
		print(Usage: MapredTimeStat.py <LocalSide> <OutputFile> <FilenameList...>)
		sys.exit()
	mapredTimeStat = MapredTimeStat(sys.argv[0], sys.argv[2:])
	mapredTimeStat.computeTimeStat()
	mapredTimeStat.dumpResultToFile(sys.argv[1])