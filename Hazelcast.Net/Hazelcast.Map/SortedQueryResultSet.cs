// Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Hazelcast.Core;
using System.Collections.Generic;
using System;
using System.Collections;
using System.Linq;

namespace Hazelcast.Map
{
    internal class SortedQueryResultSet<TKey, TValue> : SortedSet<KeyValuePair<TKey, TValue>>
    {
        private IEnumerable<KeyValuePair<TKey, TValue>> entries;
        private IterationType iterationType;

        public SortedQueryResultSet() : base(null, null)
        {
        }

        public SortedQueryResultSet(IComparer<KeyValuePair<TKey, TValue>> comparer, IEnumerable<KeyValuePair<TKey, TValue>> entries, IterationType iterationType) : base(entries, comparer)
        {
            this.entries = entries;
            this.iterationType = iterationType;
        }

        public new IEnumerator GetEnumerator()
        {
            if (entries == null)
            {
                Enumerable.Empty<KeyValuePair<TKey, TValue>>().GetEnumerator();
            }
            return new SortedEnumerator(entries, iterationType);
        }

        private sealed class SortedEnumerator : IEnumerator
        {

            private IEnumerable<KeyValuePair<TKey, TValue>> _enumerable;
            private IterationType _iterationType;

            internal SortedEnumerator(IEnumerable<KeyValuePair<TKey, TValue>> enumerable, IterationType iterationType)
            {
                this._enumerable = enumerable;
                this._iterationType = iterationType;
            }

            public object Current
            {
                get
                {
                    try
                    {
                        KeyValuePair<TKey, TValue> entry = _enumerable.GetEnumerator().Current;
                        switch (_iterationType)
                        {
                            case IterationType.KEY:
                                return entry.Key;
                            case IterationType.VALUE:
                                return entry.Value;
                            case IterationType.ENTRY:
                                return entry;
                            default:
                                throw new InvalidOperationException("Unrecognized iterationType:" + _iterationType);
                        }
                    }
                    catch (IndexOutOfRangeException)
                    {
                        throw new IndexOutOfRangeException("Index out of range, no more entries.");
                    }
                }
            }

            public bool MoveNext()
            {
                return _enumerable.GetEnumerator().MoveNext();
            }

            public void Remove()
            {
                throw new NotSupportedException();
            }

            public IEnumerator GetEnumerator()
            {
                return _enumerable.GetEnumerator();
            }

            public void Reset()
            {
                _enumerable.GetEnumerator().Reset();
            }
        }
    }
}