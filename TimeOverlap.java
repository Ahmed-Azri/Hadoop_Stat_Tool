package com.zack.stat;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.*;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Iterator;

public class TimeOverlap
{
	static SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

	String inputFileName;
	HashMap<String, DuringTimePQ> timelineToFetch;

	//Pair: String-> dst, second: src
	LinkedList<Pair<String, DuringTime>> timeline;
	LinkedList<StatTimeLineStruct> statTimelineList;
	
	boolean fileOkay;
	String[] hostIndex;

	String[] podLocalHost;

	public TimeOverlap(String inputFile)
	{
		inputFileName = inputFile;
		hostIndex = new String[4];
		hostIndex[0] = "master";
		hostIndex[1] = "slave1";
		hostIndex[2] = "slave2";
		hostIndex[3] = "slave3";

		timeline = new LinkedList<Pair<String, DuringTime>>();
		timelineToFetch = new HashMap<String, DuringTimePQ>();
		timelineToFetch.put("master", new DuringTimePQ());
		timelineToFetch.put("slave1", new DuringTimePQ());
		timelineToFetch.put("slave2", new DuringTimePQ());
		timelineToFetch.put("slave3", new DuringTimePQ());
		statTimelineList = null;

		podLocalHost = new String[2];
		podLocalHost[0] = "master";
		podLocalHost[1] = "slave1";
		try
		{
			DataInputStream in = new DataInputStream(new FileInputStream(inputFileName));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;

			String currentHost = null;
			boolean isFrom = false;
			long id = 0;
			readLineLabel:
			while((line = br.readLine()) != null)
			{
				for(String host : hostIndex)
				{
					if(line.startsWith(host + ":"))
					{
						currentHost = host;
						continue readLineLabel;
					}
				}

				if(currentHost == null)
					continue;
				if(line.equals(""))
					continue;
				if(line.startsWith("\ttotal"))
					continue;
				if(line.startsWith("\tfrom"))
				{
					isFrom = true;
					continue;
				}
				if(isFrom)
				{
					isFrom = false;
					continue;
				}
				String timelineString = line.substring(1);
				StringTokenizer strtok = new StringTokenizer(timelineString, " ");
				String startTimeStr = strtok.nextToken();
				String delim = strtok.nextToken();
				String endTimeStr = strtok.nextToken();
				delim = strtok.nextToken();
				String srcHost = strtok.nextToken();
				if(!srcHost.equals(podLocalHost[0]) && !srcHost.equals(podLocalHost[1]))
					continue;

				DuringTime startDate = new DuringTime(srcHost, startTimeStr, true, id);
				DuringTime endDate = new DuringTime(srcHost, endTimeStr, false, id);
				id++;
				
				timelineToFetch.get(currentHost).add(startDate);
				timelineToFetch.get(currentHost).add(endDate);
			}
			in.close();
			fileOkay = true;
		} catch(Exception e)
		{
			e.printStackTrace();
			fileOkay = false;
		}
	}
	public void doIt()
	{
		while(true)
		{
			//check timelineToFetch
			//fetch the earliest dt
			LinkedList<Pair<String, DuringTime>> dtList
					= new LinkedList<Pair<String,DuringTime>>();
			HashSet<String> isFetch = new HashSet<String>();

			for(String h : hostIndex)
			{
				if(timelineToFetch.get(h).isEmpty())
					continue;
				DuringTime dtToFetch = timelineToFetch.get(h).peek();
				if(dtList.size() == 0)
				{
					isFetch.add(h);
					dtList.add(new Pair<String, DuringTime>(h,dtToFetch));
					continue;
				}
				DuringTime dt = dtList.get(0).second;
				if(dtToFetch.compareTo(dt) <0)
				{
					isFetch.clear();
					isFetch.add(h);
					dtList.clear();
					dtList.add(new Pair<String, DuringTime>(h, dtToFetch));
				}
				else if(dtToFetch.compareTo(dt) == 0)
				{
					isFetch.add(h);
					dtList.add(new Pair<String, DuringTime>(h, dtToFetch));
				}
			}

			if(dtList.isEmpty())
				break;

			dtList.clear();
			for(String fetchHost : isFetch)
			{
				DuringTime dtToFetch = timelineToFetch.get(fetchHost).pop();
				dtList.add(new Pair<String, DuringTime>(fetchHost,dtToFetch));
			}
			sort(dtList);
			//check if the dt interrupt the duration of a transmission
			if(timeline.isEmpty())
				timeline.addAll(dtList);
			else
			{
				int index = 0;
				for(int i=0;i<timeline.size();i++)
				{
					Pair<String, DuringTime> p = timeline.get(i);
					if(p.second.compareTo(dtList.get(0).second) <0)
					{
						index = 0;
						break;
					}
				}
				timeline.addAll(index, dtList);
				sort(timeline);
			}
		}

		// 
		statTimeline();
	}
	public void statTimeline()
	{
		if(timeline.isEmpty())
			return;
		Date currentDate = timeline.get(0).second.getDate();
		int flowCount = 0;
		int podLocalFlowCount01 = 0;
		int podLocalFlowCount10 = 0;
		int flowInSecond = 0;
		int podLocalFlowInSecond01 = 0;
		int podLocalFlowInSecond10 = 0;

		LinkedList<Pair<String, DuringTime>> startDTList = new LinkedList<Pair<String, DuringTime>>();
		statTimelineList = new LinkedList<StatTimeLineStruct>();
		for(Pair<String, DuringTime> p : timeline)
		{
			String dst = p.first;
			DuringTime dt = p.second;
			boolean timeEqual = currentDate.equals(dt.getDate());

			if(dt.isStart())
			{
				if(!timeEqual)
				{
					StatTimeLineStruct stls = new StatTimeLineStruct(currentDate, dt.getDate(), 
							flowCount, flowInSecond, podLocalFlowCount01, podLocalFlowCount10);
					statTimelineList.add(stls);
					currentDate = dt.getDate();
				}
				flowCount++;
				if((dst.equals(podLocalHost[0]) && dt.getRemote().equals(podLocalHost[1])))
				{
					podLocalFlowCount10++;
				}
				if((dst.equals(podLocalHost[1]) && dt.getRemote().equals(podLocalHost[0])))
				{
					podLocalFlowCount01++;
				}
				startDTList.add(p);
			}
			else
			{
				//check 
				Pair<String, DuringTime> theStartOne = null;
				for(Pair<String, DuringTime> startElement : startDTList)
				{
					if(startElement.first.equals(dst) && startElement.second.getRemote().equals(dt.getRemote()))
					{
						theStartOne = startElement;
						startDTList.remove(startElement);
						break;
					}
				}
				if(theStartOne == null)
				{
					//fail
					statTimelineList = null;
					return;
				}
				if(!timeEqual)
				{
					StatTimeLineStruct stls = new StatTimeLineStruct(currentDate, dt.getDate(), 
							flowCount, flowInSecond, 
							podLocalFlowCount01 + podLocalFlowInSecond01, podLocalFlowCount10 + podLocalFlowInSecond10);
					statTimelineList.add(stls);
					currentDate = dt.getDate();
					flowInSecond = 0;
					podLocalFlowInSecond10 = 0;
					podLocalFlowInSecond01 = 0;
				}
				flowCount--;
				if((dst.equals(podLocalHost[0]) && dt.getRemote().equals(podLocalHost[1])))
				{
					podLocalFlowCount10--;
				}
				if((dst.equals(podLocalHost[1]) && dt.getRemote().equals(podLocalHost[0])))
				{
					podLocalFlowCount01--;
				}
				if(theStartOne.second.getDate().equals(dt.getDate()))
				{
					if((dst.equals(podLocalHost[0]) && dt.getRemote().equals(podLocalHost[1])))
					{
						podLocalFlowInSecond10++;
					}
					if((dst.equals(podLocalHost[1]) && dt.getRemote().equals(podLocalHost[0])))
					{
						podLocalFlowInSecond01++;
					}
					flowInSecond++;
				}
			}
		}
	}
	public void dumpToFile(String outputFile)
	{
		try
		{
			FileWriter fstream = new FileWriter(outputFile);
			BufferedWriter out = new BufferedWriter(fstream);

			for(Pair<String, DuringTime> p : timeline)
				out.write(p.first + " receive from "+ p.second.toString() + "\n");
			if(statTimelineList != null)
			{
				out.write("\n\n");
				out.write("Time                FlowCount podLocalCount " 
						+ podLocalHost[0] + "-" + podLocalHost[1] + " " + podLocalHost[1] + "-" + podLocalHost[0] + "\n");
				for(StatTimeLineStruct stls: statTimelineList)
					out.write(stls.toString() + "\n");
			}
			out.close();
		} catch(Exception e)
        {
			System.out.println("Write to file " + outputFile + " error. " + e.getMessage());
		}
	}
	public boolean isFileOkay()
	{
		return fileOkay;
	}
	public void sort(LinkedList<Pair<String, DuringTime>> dtList)
	{
		@SuppressWarnings("unchecked")
		LinkedList<Pair<String, DuringTime>> dtListTmp 
			= (LinkedList<Pair<String, DuringTime>>) dtList.clone();
		dtList.clear();
		while(!dtListTmp.isEmpty())
		{
			Pair<String, DuringTime> p = dtListTmp.peekFirst();
			int index = 0;
			for(int i=0;i<dtListTmp.size();i++)
			{
				if(p.second.compareTo(dtListTmp.get(i).second) >0)
				{
					p = dtListTmp.get(i);
					index = i;
				}
			}
			dtListTmp.remove(index);
			dtList.add(p);
		}
	}
	class StatTimeLineStruct
	{
		Date startDate;
		Date endDate;
		int flowCount;
		int localCount;
		String startDateStr;
		String endDateStr;
		int from_0_to_1;
		int from_1_to_0;
		public StatTimeLineStruct(Date startDate, Date endDate, int flowCount, 
								int flowInSecond, int from_0_to_1, int from_1_to_0)
		{
			this.startDate = startDate;
			this.endDate = endDate;
			this.flowCount = flowCount + flowInSecond;
			this.localCount = from_0_to_1 + from_1_to_0;
			startDateStr = sdf.format(startDate);
			endDateStr = sdf.format(endDate);
			this.from_0_to_1 = from_0_to_1;
			this.from_1_to_0 = from_1_to_0;
		}

		@Override
		public String toString()
		{
			return startDateStr + " - " + endDateStr + "     " + flowCount + "           " + localCount 
				+ "             " + from_0_to_1 + "             " + from_1_to_0;
		}
	}
	public static void main(String[] args)
	{
		if(args.length != 1)
		{
			System.out.println("Usage: TimeOverlap <Filename>");
			return;
		}

		TimeOverlap timeOverlap = new TimeOverlap(args[0]);
		if(timeOverlap.isFileOkay())
		{
			timeOverlap.doIt();
			timeOverlap.dumpToFile(args[0]+"_timeline");
		}
	}
}
