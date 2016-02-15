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
using Hazelcast.Map;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Hazelcast.Util
{
    internal sealed class SortingUtil
    {
        public static SortedQueryResultSet<TKey, TValue> GetSortedQueryResultSet<TKey, TValue>(ISet<KeyValuePair<TKey, TValue>> list,
                                                                PagingPredicate<TKey, TValue> pagingPredicate, IterationType iterationType)
        {
            if (list == null || list.Count == 0)
            {
                return new SortedQueryResultSet<TKey, TValue>();
            }
            IComparer<KeyValuePair<TKey, TValue>> comparer = SortingUtil.NewComparator(pagingPredicate.GetComparator(), iterationType);
            List<KeyValuePair<TKey, TValue>> sortedList = new List<KeyValuePair<TKey, TValue>>();
            sortedList.AddRange(list);
            sortedList.Sort(comparer);

            KeyValuePair<int, KeyValuePair<TKey, TValue>> nearestAnchorEntry = pagingPredicate.GetNearestAnchorEntry();
            int nearestPage = nearestAnchorEntry.Key;
            int page = pagingPredicate.GetPage();
            int pageSize = pagingPredicate.GetPageSize();
            int begin = pageSize * (page - nearestPage - 1);
            int size = sortedList.Count;
            if (begin > size)
            {
                return new SortedQueryResultSet<TKey, TValue>();
            }
            int end = begin + pageSize;
            if (end > size)
            {
                end = size;
            }

            SetAnchor(sortedList, pagingPredicate, nearestPage);
            List<KeyValuePair<TKey, TValue>> subList = sortedList.GetRange(begin, end);
            return new SortedQueryResultSet<TKey, TValue>(comparer, subList, iterationType);
        }

        private static void SetAnchor<TKey, TValue>(List<KeyValuePair<TKey, TValue>> list, PagingPredicate<TKey, TValue> pagingPredicate, int nearestPage)
        {
            if (list.Count == 0)
            {
                return;
            }

            int size = list.Count;
            int pageSize = pagingPredicate.GetPageSize();
            for (int i = pageSize; i <= size; i += pageSize)
            {
                KeyValuePair<TKey, TValue> anchor = list[i - 1];
                nearestPage++;
                pagingPredicate.SetAnchor(nearestPage, anchor);
            }
        }
        
        public static IComparer<KeyValuePair<TKey, TValue>> NewComparator<TKey, TValue>(IComparer<KeyValuePair<TKey, TValue>> comparator,
                                                      IterationType iterationType)
        {
            return new SortingComparer<TKey, TValue>(comparator, iterationType);
        }

        public static IComparer<KeyValuePair<TKey, TValue>> NewComparator<TKey, TValue>(PagingPredicate<TKey, TValue> pagingPredicate)
        {
            return new SortingComparer<TKey, TValue>(pagingPredicate.GetComparator(), pagingPredicate.GetIterationType());
        }

        public static int Compare<TKey, TValue>(IComparer<KeyValuePair<TKey, TValue>> comparer, IterationType iterationType, 
            KeyValuePair<TKey, TValue> entry1, KeyValuePair<TKey, TValue> entry2)
        {
            if (comparer != null)
            {
                int compareResults = comparer.Compare(entry1, entry2);
                if (compareResults != 0)
                {
                    return compareResults;
                }
                return Comparer<int>.Default.Compare(entry1.Key.GetHashCode(), entry2.Key.GetHashCode());
            }

            object comparable1;
            object comparable2;
            switch (iterationType)
            {
                case IterationType.KEY:
                    comparable1 = entry1.Key;
                    comparable2 = entry2.Key;
                    break;
                case IterationType.VALUE:
                    comparable1 = entry1.Value;
                    comparable2 = entry2.Value;
                    break;
                default:
                    // Possibly ENTRY
                    // If entries are comparable, we can compare them
                    if (entry1 is IComparable && entry2 is IComparable) {
                        comparable1 = entry1;
                        comparable2 = entry2;
                    } else {
                        // Otherwise, comparing entries directly is not meaningful.
                        // So keys can be used instead of map entries.
                        comparable1 = entry1.Key;
                        comparable2 = entry2.Key;
                    }
                    break;
            }

            checkIfComparable(comparable1);
            checkIfComparable(comparable2);

            int result = ((IComparable)comparable1).CompareTo(comparable2);
            if (result != 0)
            {
                return result;
            }
            return Comparer<int>.Default.Compare(entry1.Key.GetHashCode(), entry2.Key.GetHashCode());
        }

        private static void checkIfComparable(Object comparable)
        {
            if (comparable is IComparable) {
                return;
            }
            throw new ArgumentException("Not comparable " + comparable);
        }


        internal class SortingComparer<TKey, TValue> : IComparer<KeyValuePair<TKey, TValue>>
        {
            private IComparer<KeyValuePair<TKey, TValue>> _comparer;
            private IterationType _iterationType;

            public SortingComparer(IComparer<KeyValuePair<TKey, TValue>> comparer, IterationType iterationType)
            {
                this._comparer = comparer;
                this._iterationType = iterationType;
            }

            public int Compare(KeyValuePair<TKey, TValue> entry1, KeyValuePair<TKey, TValue> entry2)
            {
                return SortingUtil.Compare(_comparer, _iterationType, entry1, entry2);
            }
        }
    }
}