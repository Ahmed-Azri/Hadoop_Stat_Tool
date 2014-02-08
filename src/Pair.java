package com.zack.stat;

public class Pair<F extends Comparable<? super F>, S extends Comparable<? super S>> implements Comparable<Pair<F, S>>
{
	F first;
	S second;
	public Pair(F f, S s)
	{
		first = f;
		second = s;
	}
	@Override
	public int compareTo(Pair<F, S> o)
	{
		int cmp = compare(first, o.first);
		return cmp == 0 ? compare(second, o.second) : cmp;
	}
	private <T extends Comparable<? super T> >int compare(T o1, T o2)
	{
		if(o1 == null)
		{
			if(o2 == null)
				return 0;
			return -1;
		}
		if(o2 == null)
			return 1;

		// o1 and o2 are not null both
		return ((Comparable<? super T>) o1).compareTo(o2);
	}
	@Override
	public boolean equals(Object obj)
	{
		if (!(obj instanceof Pair))
			return false;
		if (this == obj)
			return true;
		return equal(first, ((Pair) obj).first)
				&& equal(second, ((Pair) obj).second);
	}
	private boolean equal(Object o1, Object o2)
	{
		return o1 == null ? o2 == null : (o1 == o2 || o1.equals(o2));
	}
}