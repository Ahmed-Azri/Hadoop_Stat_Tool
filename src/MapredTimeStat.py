import datetime
import sys

class MapredTimeStat:
	POD_TOTAL_NUM = 4
	RACK_NUM_PER_POD = 2
	SERVER_NUM_PER_RACK = 2

	def __init__(self, localSide, filenameList):
		self.localSide = localSide
		self.filenameList = filenameList
		self.totalReceiveByte = 0
		self.totalNonLocalReceiveByte = 0
	def computeTimeStat(self):
	def dumpResultToFile(self, outputFile):

def main():
	if len(sys.argv) < 3:
		print(Usage: MapredTimeStat.py <LocalSide> <OutputFile> <FilenameList...>)
		sys.exit()
	mapredTimeStat = MapredTimeStat(sys.argv[0], sys.argv[2:])
	mapredTimeStat.computeTimeStat()
	mapredTimeStat.dumpResultToFile(sys.argv[1])