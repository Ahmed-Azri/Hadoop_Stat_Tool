package com.zack.stat;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.Iterator;

class DuringTime implements Comparable<DuringTime>
{
//	static SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

	String dateString;
	Date date;
	boolean start;
	String remote;
	long id;
	public DuringTime(String remote, String dateString, boolean isStart, long id)
	{
		this.remote = remote;
		this.dateString = dateString;
		start = isStart;
		this.id = id;
		try
		{
			date = StatConst.sdf.parse(dateString);
		} catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public String getRemote()
	{
		return remote;
	}
	public boolean isStart()
	{
		return start;
	}
	public String getDateString()
	{
		return dateString;
	}
	public Date getDate()
	{
		return date;
	}
	@Override
	public String toString()
	{
		return remote + ": " + dateString + (start ? " Start" : " End");
	}
	@Override
	public int compareTo(DuringTime d)
	{
		if(dateString.startsWith("23:") && d.dateString.startsWith("00:"))
			return -1;
		if(d.dateString.startsWith("23:") && dateString.startsWith("00:"))
			return 1;
		int cmp = dateString.compareTo(d.dateString);
		if(cmp == 0)
		{
			if(start && !d.start)
			{
				if(id != d.id)
					return 1;
				return -1;
			}
			if(!start && d.start)
			{
				if(id == d.id)
					return 1;
				return -1;
			}
		}
		return cmp;
	}
	@Override
	public boolean equals(Object obj)
	{
		if(this == obj)
			return true;
		if(obj == null || this == null)
			return obj == this;
		if(getClass() != obj.getClass())
			return false;

		DuringTime other = (DuringTime)obj;
		boolean eqStr = false;
		boolean eqDate = false;
		boolean eqRemote = false;
		if(dateString == null)
		{
			if(other.dateString != null)
				return false;
			eqStr = true;
		}
		else
			eqStr = dateString.equals(other.dateString);

		if(date == null)
		{
			if(other.date != null)
				return false;
			eqDate = true;
		}
		else
			eqDate = date.equals(other.date);
		if(remote == null)
		{
			if(other.remote != null)
				return false;
			eqRemote = true;
		}
		else
			eqRemote = remote.equals(other.remote);
		return eqStr && eqDate && eqRemote && (start == other.start);
	}
}
class DuringTimePQ implements Iterable<DuringTime>
{
	LinkedList<DuringTime> list;
	public DuringTimePQ()
	{
		list = new LinkedList<DuringTime>();
	}
	public Iterator<DuringTime> iterator()
	{
		Iterator<DuringTime> it = list.iterator();
		return it;
	}
	public void add(DuringTime e)
	{
		int startCount = 0;
		int endCount = 0;
		int completeCount = 0;
		for(int i=0;i<list.size();i++)
		{
			if(list.get(i).isStart())
				startCount++;
			else
				endCount++;
			if(e.compareTo(list.get(i)) <0)
			{
				list.add(i, e);
				return;
			}
		}
		list.add(e);
	}
	public void remove(DuringTime e)
	{
		for(int i=0;i<list.size();i++)
			if(e.equals(list.get(i)))
			{
				list.remove(i);
				return;
			}
	}
	public DuringTime pop()
	{
		if(list.isEmpty())
			return null;
		return list.removeFirst();
	}
	public DuringTime peek()
	{
		if(list.isEmpty())
			return null;
		return list.get(0);
	}
	public int size()
	{
		return list.size();
	}
	public boolean isEmpty()
	{
		return list.isEmpty();
	}
	public DuringTime get(int i)
	{
		if(list.size() < i)
			return null;
		return list.get(i);
	}
}
