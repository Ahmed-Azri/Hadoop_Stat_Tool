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

/*
 * This class list the data transferring timestamp in increasing order, and
 * show that in which time there are some overlap flow.
 * For the overlap flow, we seperate the flow into pod-local flow and non-pod-local flow
 */
public class TimeOverlap
{
	String inputFileName;

	HashMap<String, DuringTimePQ> timelineToFetch;

	LinkedList<Pair<String, DuringTime>> timeline;

	LinkedList<StatTimeLineStruct> statTimelineList;
	
	boolean fileOkay;

	public TimeOverlap(String inputFile)
	{
		/* Here we read the file and record the data transferring timestamp
		 * inputfile format:
		 * receiver1:
		 *     total: total_received_bytes
		 *     total_no_local: total_no_local_received_bytes
		 *     from sender1:
		 *	       received_byte1
		 *	   from sender2:
		 *	       received_byte2
		 *     start_time1 - end_time1 : sender1
		 *     start_time2 - end_time2 : sender2
		 *     ...
		 *
		 * receiver2:
		 *     ...
		 */
		inputFileName = inputFile;

		timeline = new LinkedList<Pair<String, DuringTime>>();
		timelineToFetch = new HashMap<String, DuringTimePQ>();
		for(String host : StatConst.host)
			timelineToFetch.put(host, new DuringTimePQ());
		statTimelineList = null;

		try
		{
			DataInputStream in = new DataInputStream(new FileInputStream(inputFileName));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;

			//current processing receiver
			String currentReceiver = null;
			boolean isFrom = false;
			long id = 0;

			readLineLabel:
			while((line = br.readLine()) != null)
			{
				//check if we have finish a host, and ready to
				//count another host (i.e. we read a line such as "host1:").
				for(String host : StatConst.host)
				{
					if(line.startsWith(host + ":"))
					{
						currentReceiver = host;
						continue readLineLabel;
					}
				}

				//ignore all the message that is not the timestamp
				if(currentReceiver == null)
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

				//process the line that recored the timestamp
				//fetch the token in the format: start_time - end_time : sender
				String timelineString = line.substring(1);
				StringTokenizer strtok = new StringTokenizer(timelineString, " ");
				String startTimeStr = strtok.nextToken();
				String delim = strtok.nextToken();
				String endTimeStr = strtok.nextToken();
				delim = strtok.nextToken();
				String sender = strtok.nextToken();

				//we only trace the transferring that sender is in podLocalHost
				//Note that if we want to trace all the transferring, remove this statement
				if(!sender.equals(StatConst.podLocalHost[0]) && !sender.equals(StatConst.podLocalHost[1]))
					continue;

				DuringTime startDate = new DuringTime(sender, startTimeStr, true, id);
				DuringTime endDate = new DuringTime(sender, endTimeStr, false, id);
				id++;
				
				timelineToFetch.get(currentReceiver).add(startDate);
				timelineToFetch.get(currentReceiver).add(endDate);
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
			/*
			 * 1. check the earliest DuringTime in timelineToFetch, record it in isFetch
			 * 2. fetch the DuringTime according to isFetch
			 * 3. sort the fetch result
			 * 4. insert the result to timeline instance
			 */

			//we use list to hold the fetch result from timelineToFetch,
			//because there may be many transferring start/end at the same time
			LinkedList<Pair<String, DuringTime>> dtList
					= new LinkedList<Pair<String,DuringTime>>();

			//the instance recording the set of host that has earliest DuringTime
			HashSet<String> isFetch = new HashSet<String>();

			//step 1
			for(String h : StatConst.host)
			{
				//we iterate each host's queue, check that if the 
				//head of the queue is earliest one
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

			//no DuringTime can be fetch
			if(dtList.isEmpty())
				break;

			//step 2
			dtList.clear();
			for(String fetchHost : isFetch)
			{
				DuringTime dtToFetch = timelineToFetch.get(fetchHost).pop();
				dtList.add(new Pair<String, DuringTime>(fetchHost,dtToFetch));
			}

			//step 3
			sort(dtList);

			//step 4
			if(timeline.isEmpty())
				timeline.addAll(dtList);
			else
			{
				//check if the dt interrupt the duration of a transmission
				//this may be redundant, because we sort the timeline at the end
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

		//compute the overlap flows
		statTimeline();
	}
	public void statTimeline()
	{
		if(timeline.isEmpty())
			return;

		//TODO: this need to be modify to make podLocalFlowCount generate
		//and debug the flow in second
		/*
		 * currentDate : current timestamp we stat
		 * flowCount : the total num of flows at currentDate
		 * podLocalFlowCount01 : the local num of flows from podLocalHost[0] to podLocalHost[1]
		 * flowInSecond: 
		 * */ 
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
				if((dst.equals(StatConst.podLocalHost[0]) 
					&& dt.getRemote().equals(StatConst.podLocalHost[1])))
				{
					podLocalFlowCount10++;
				}
				if((dst.equals(StatConst.podLocalHost[1]) 
					&& dt.getRemote().equals(StatConst.podLocalHost[0])))
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
					if(startElement.first.equals(dst) 
						&& startElement.second.getRemote().equals(dt.getRemote()))
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
					StatTimeLineStruct stls 
							= new StatTimeLineStruct(currentDate, dt.getDate(), 
							flowCount, flowInSecond, 
							podLocalFlowCount01 + podLocalFlowInSecond01, 
							podLocalFlowCount10 + podLocalFlowInSecond10);
					statTimelineList.add(stls);
					currentDate = dt.getDate();
					flowInSecond = 0;
					podLocalFlowInSecond10 = 0;
					podLocalFlowInSecond01 = 0;
				}
				flowCount--;
				if((dst.equals(StatConst.podLocalHost[0]) 
					&& dt.getRemote().equals(StatConst.podLocalHost[1])))
				{
					podLocalFlowCount10--;
				}
				if((dst.equals(StatConst.podLocalHost[1]) 
					&& dt.getRemote().equals(StatConst.podLocalHost[0])))
				{
					podLocalFlowCount01--;
				}
				if(theStartOne.second.getDate().equals(dt.getDate()))
				{
					if((dst.equals(StatConst.podLocalHost[0]) 
						&& dt.getRemote().equals(StatConst.podLocalHost[1])))
					{
						podLocalFlowInSecond10++;
					}
					if((dst.equals(StatConst.podLocalHost[1]) 
						&& dt.getRemote().equals(StatConst.podLocalHost[0])))
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
						+ StatConst.podLocalHost[0] + "-" 
						+ StatConst.podLocalHost[1] 
						+ " " + StatConst.podLocalHost[1] 
						+ "-" + StatConst.podLocalHost[0] + "\n");
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

	/*
	 * The class to record the stat of overlap flows
	 * TODO: This should be modified to make generate
	 */
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
			startDateStr = StatConst.sdf.format(startDate);
			endDateStr = StatConst.sdf.format(endDate);
			this.from_0_to_1 = from_0_to_1;
			this.from_1_to_0 = from_1_to_0;
		}

		@Override
		public String toString()
		{
			return startDateStr + " - " + endDateStr 
				+ "     " + flowCount 
				+ "           " + localCount 
				+ "             " 
				+ from_0_to_1 + "             " + from_1_to_0;
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
