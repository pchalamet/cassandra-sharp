using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CassandraSharp.Extensibility;
using System.Diagnostics;
using System.Threading.Tasks;

namespace CassandraSharp.Instrumentation
{
	public abstract class AbstractInstrumentation : IInstrumentation
	{
		public ITimer CreateTimer(string cqlQuery)
		{
			return new Timer(cqlQuery, this);
		}

		public abstract void ExecuteQuery(string _cql, ITimer timer);
		public abstract void ErrorInQuery(string _cql, ITimer timer);
		public abstract void GetConnection(ITimer timer);
		public abstract void PrepareQuery(string _cql, ITimer timer);

		private class Timer : ITimer
		{
			private Stopwatch stopwatch = new Stopwatch();
			private List<Task> tasks = new List<Task>();

			private readonly string cql;
			private readonly AbstractInstrumentation parent;

			public Timer(string cql, AbstractInstrumentation parent)
			{
				this.cql = cql;
				this.parent = parent;
			}

			~Timer()
			{
				stopwatch.Stop();
				if (cql != null)
				{
					foreach (Task t in tasks)
					{
						if (t.IsFaulted)
						{
							parent.ErrorInQuery(cql, this);
							return;
						}
					}
					parent.ExecuteQuery(cql, this);
				}
			}

			public void Start()
			{
				stopwatch.Start();
			}

			public void Stop()
			{
				stopwatch.Stop();
			}

			public void AddTask(Task task)
			{
				tasks.Add(task);
			}

			public long ElapsedMilliseconds
			{
				get { return stopwatch.ElapsedMilliseconds; }
			}
		}
	}
}
