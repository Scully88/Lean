/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

using System;
using System.Collections.Generic;
using System.Threading;

namespace QuantConnect.Lean.Engine.DataFeeds
{
    /// <summary>
    /// 
    /// </summary>
    public class WeightedTaskScheduler
    {
        private readonly List<WorkQueue> _work = new List<WorkQueue>();

        /// <summary>
        /// Singleton instance
        /// </summary>
        public static WeightedTaskScheduler Instance = new WeightedTaskScheduler();

        private WeightedTaskScheduler()
        {
            for (var i = 0; i < Environment.ProcessorCount / 4; i++)
            {
                _work.Add(new WorkQueue());
                var workQueue = _work[i];
                var thread = new Thread(() => WorkerThread(workQueue))
                {
                    IsBackground = true,
                    Priority = ThreadPriority.Lowest,
                    Name = $"WeightedWorkThread{i}"
                };
                thread.Start();
            }

            var queueManager = new Thread(() =>
            {
                while (true)
                {
                    Thread.Sleep(TimeSpan.FromMilliseconds(50));
                    foreach (var queue in _work)
                    {
                        queue.Sort();
                    }
                }
            })
            {
                IsBackground = true,
                Name = "WeightedWorkQueueManager",
                Priority = ThreadPriority.Lowest
            };
            queueManager.Start();
        }

        public void QueueWork(Func<bool> workFunc, Func<int> weightFunc)
        {
            WorkQueue workQueue = null;
            var queueCount = int.MaxValue;
            foreach (var potentialQueue in _work)
            {
                if (potentialQueue.Count < queueCount)
                {
                    queueCount = potentialQueue.Count;
                    workQueue = potentialQueue;
                    if (queueCount == 0)
                    {
                        break;
                    }
                }
            }

            if (workQueue == null)
            {
                throw new InvalidOperationException("WeightedTaskScheduler.QueueWork(): WorkQueue is null");
            }
            workQueue.Add(new WorkItem(workFunc, weightFunc));
        }

        private static void WorkerThread(WorkQueue workQueue)
        {
            while (true)
            {
                var workItem = workQueue.GetAndRemove();
                if (workItem == null)
                {
                    Thread.Sleep(TimeSpan.FromMilliseconds(1));
                    continue;
                }

                try
                {
                    if (workItem.Work())
                    {
                        workQueue.Add(workItem);
                    }
                }
                catch (Exception exception)
                {
                    Logging.Log.Error(exception);
                }
            }
        }

        private class WorkQueue
        {
            private readonly List<WorkItem> _workQueue;

            public int Count;

            public WorkQueue()
            {
                _workQueue = new List<WorkItem>();
            }

            public void Add(WorkItem work)
            {
                lock (_workQueue)
                {
                    _workQueue.Add(work);
                    Interlocked.Increment(ref Count);
                }
            }

            public void Sort()
            {
                lock (_workQueue)
                {
                    foreach (var item in _workQueue)
                    {
                        item.UpdateWeight();
                    }
                    _workQueue.Sort();
                }
            }

            public WorkItem GetAndRemove()
            {
                WorkItem potentialWorkItem = null;
                lock (_workQueue)
                {
                    if (_workQueue.Count != 0)
                    {
                        potentialWorkItem = _workQueue[0];

                        if (potentialWorkItem.ShouldWork())
                        {
                            _workQueue.RemoveAt(0);
                            Interlocked.Decrement(ref Count);
                        }
                    }
                }
                return potentialWorkItem;
            }
        }

        private class WorkItem : IComparable
        {
            private readonly Func<int> _weightFunc;
            private int _weight;

            public Func<bool> Work { get; }

            public WorkItem(Func<bool> work, Func<int> weightFunc)
            {
                Work = work;
                _weightFunc = weightFunc;
            }

            public void UpdateWeight()
            {
                var newWeight = _weightFunc();
                if (newWeight >= 500)
                {
                    // the upper threshold will stop the worker from loading more data. This is roughly 1 GB
                    _weight = int.MaxValue;
                }
                _weight = newWeight;
            }

            public bool ShouldWork()
            {
                if (_weight == int.MaxValue)
                {
                    UpdateWeight();
                }
                return _weight != int.MaxValue;
            }

            public int CompareTo(object obj)
            {
                var other = obj as WorkItem;
                if (ReferenceEquals(this, obj))
                {
                    return 0;
                }
                // By definition, any object compares greater than null
                if (ReferenceEquals(this, null))
                {
                    return -1;
                }
                if (ReferenceEquals(null, other))
                {
                    return 1;
                }

                return _weight.CompareTo(other._weight);
            }
        }
    }
}
