using LinqToDB.DataProvider.Firebird;
using System;

namespace Tests
{
	public class Scope : IDisposable
	{
		private readonly Action _onExit;

		public Scope(Action onEnter, Action onExit)
		{
			onEnter();
			_onExit = onExit;
		}

		void IDisposable.Dispose()
		{
			_onExit();
		}
	}
}
