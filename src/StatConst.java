package com.zack.stat;

import java.text.SimpleDateFormat;

public class StatConst
{
	static final int podTotalNum = 2;
	static final int hostTotalNum = 4;
	static int podHostNum = hostTotalNum/podTotalNum;

	static String[] host;
	static String[] podLocalHost;
	static int podLocalIndex;

	static SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
	static
	{
		podLocalIndex = 0;
		host = new String[hostTotalNum];
		host[0] = "master";
		host[1] = "slave1";
		host[2] = "slave2";
		host[3] = "slave3";

		podLocalHost = new String[hostTotalNum/podTotalNum];
		for(int i=0;i<podHostNum;i++)
			podLocalHost[i] = host[podHostNum * podLocalIndex + i];
	}
}
