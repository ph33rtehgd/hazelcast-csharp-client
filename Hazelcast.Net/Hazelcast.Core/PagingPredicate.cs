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

using Hazelcast.IO;
using Hazelcast.IO.Serialization;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Hazelcast.Core
{
    /// <summary>
    /// Paging Predicate
    /// </summary>
    public class PagingPredicate<TKey, TValue> : IPredicate<TKey, TValue>
    {
        private static readonly KeyValuePair<int, KeyValuePair<TKey, TValue>> NULL_ANCHOR = new KeyValuePair<int, KeyValuePair<TKey, TValue>>(-1, new KeyValuePair<TKey, TValue>());

        private IList<KeyValuePair<int, KeyValuePair<TKey, TValue>>> _anchorList;
        private int _pageSize;
        private int _page { get; set; }
        private IPredicate<TKey, TValue> _predicate;
        private IComparer<KeyValuePair<TKey, TValue>> _comparer;
        private IterationType _iterationType;

        public PagingPredicate()
        {
            _anchorList = new List<KeyValuePair<int, KeyValuePair<TKey, TValue>>>();
        }

        /// <summary>
        /// Construct with a pageSize results will not be filtered
        /// results will be natural ordered
        /// </summary>
        /// <param name="pageSize"></param>
        public PagingPredicate(int pageSize)
        {
            if (pageSize <= 0)
            {
                throw new ArgumentException("pageSize should be greater than 0 !!!");
            }
            _pageSize = pageSize;
            _anchorList = new List<KeyValuePair<int, KeyValuePair<TKey, TValue>>>(_pageSize);
        }

        /// <summary>
        /// Construct with an inner predicate and pageSize
        /// results will be filtered via inner predicate
        /// results will be natural ordered
        /// </summary>
        /// <param name="predicate"></param>
        /// <param name="pageSize"></param>
        public PagingPredicate(IPredicate<TKey, TValue> predicate, int pageSize) : this(pageSize)
        {
            SetInnerPredicate(predicate);
        }

        /// <summary>
        /// Construct with a comparator and pageSize
        /// results will not be filtered 
        /// results will be ordered via comparator
        /// </summary>
        /// <param name="comparer"></param>
        /// <param name="pageSize"></param>
        public PagingPredicate(IComparer<KeyValuePair<TKey, TValue>> comparer, int pageSize) : this(pageSize)
        {
            _comparer = comparer;
        }

        /// <summary>
        /// Construct with an inner predicate, comparator and pageSize
        /// results will be filtered via inner predicate
        /// results will be ordered via comparator
        /// </summary>
        /// /// <param name="predicate"></param>
        /// <param name="comparer"></param>
        /// <param name="pageSize"></param>
        public PagingPredicate(IPredicate<TKey, TValue> predicate, IComparer<KeyValuePair<TKey, TValue>> comparer, int pageSize) : this(pageSize)
        {
            SetInnerPredicate(predicate);
            _comparer = comparer;
        }

        private void SetInnerPredicate(IPredicate<TKey, TValue> predicate)
        {
            if (predicate is PagingPredicate<TKey, TValue>)
            {
                throw new ArgumentException("Nested PagingPredicate is not supported!!!");
            }
            _predicate = predicate;
        }

        /// <summary>
        /// Resets the predicate for reuse.
        /// </summary>
        public void Reset()
        {
            _iterationType = IterationType.KEY;
            _anchorList.Clear();
            _page = 0;
        }

        /// <summary>
        /// Sets the page value to the next page.
        /// </summary>
        public void NextPage()
        {
            _page++;
        }

        /// <summary>
        /// Sets the page value to the previous page.
        /// </summary>
        public void PreviousPage()
        {
            if (_page != 0)
            {
                _page--;
            }
        }

        public IterationType GetIterationType()
        {
            return _iterationType;
        }

        public void SetIterationType(IterationType iterationType)
        {
            this._iterationType = iterationType;
        }

        public void SetPage(int page)
        {
            _page = page;
        }

        public int GetPage()
        {
            return _page;
        }

        public int GetPageSize()
        {
            return _pageSize;
        }

        public IPredicate<TKey, TValue> GetPredicate()
        {
            return _predicate;
        }

        public IComparer<KeyValuePair<TKey, TValue>> GetComparator()
        {
            return _comparer;
        }

        /// <summary>
        /// Retrieve the anchor object which is the last value object on the previous page.
        /// Note: This method will return a default DictionaryEntry on the first page of the query result.
        /// </summary>
        public KeyValuePair<TKey, TValue> GetAnchor()
        {
            try
            {
                KeyValuePair<int, KeyValuePair<TKey, TValue>> anchorEntry = _anchorList[_page];
                return anchorEntry.Value;
            }
            catch(ArgumentOutOfRangeException)
            {
                return default(KeyValuePair<TKey, TValue>);
            }
        }

        /// <summary>
        /// After each query, an anchor entry is set for that page.
        /// The anchor entry is the last entry of the query.
        /// </summary>
        internal void SetAnchor(int page, KeyValuePair<TKey, TValue> anchor)
        {
            KeyValuePair<int, KeyValuePair<TKey, TValue>> anchorEntry = new KeyValuePair<int, KeyValuePair<TKey, TValue>>(page, anchor);
            int anchorCount = _anchorList.Count;
            if (page < anchorCount)
            {
                _anchorList[page] = anchorEntry;
            }
            else if (page == anchorCount)
            {
                _anchorList.Add(anchorEntry);
            }
            else {
                throw new ArgumentException("Anchor index is not correct, expected: " + page + " found: " + anchorCount);
            }
        }

        /// <summary>
        /// After each query, an anchor entry is set for that page. see
        /// For the next query user may set an arbitrary page.
        /// For example: user queried first 5 pages which means first 5 anchor is available
        /// if the next query is for the 10th page then the nearest anchor belongs to page 5
        /// but if the next query is for the 3nd page then the nearest anchor belongs to page 2
        /// </summary>
        internal KeyValuePair<int, KeyValuePair<TKey, TValue>> GetNearestAnchorEntry()
        {
            int anchorCount = _anchorList.Count;
            if (_page == 0 || anchorCount == 0)
            {
                return NULL_ANCHOR;
            }

            KeyValuePair<int, KeyValuePair<TKey, TValue>> anchoredEntry;
            if (_page < anchorCount)
            {
                anchoredEntry = _anchorList[_page - 1];
            }
            else {
                anchoredEntry = _anchorList[anchorCount - 1];
            }
            return anchoredEntry;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="output"></param>
        public void WriteData(IObjectDataOutput output)
        {
            output.WriteObject(_predicate);
            output.WriteObject(_comparer);
            output.WriteInt(_page);
            output.WriteInt(_pageSize);
            output.WriteUTF(_iterationType.ToString());

            if (_anchorList != null)
            {
                output.WriteInt(_anchorList.Count);
                foreach (KeyValuePair<int, KeyValuePair<TKey, TValue>> anchor in _anchorList)
                {
                    output.WriteInt(anchor.Key);
                    KeyValuePair<TKey, TValue> anchorEntry = anchor.Value;
                    output.WriteObject(anchorEntry.Key);
                    output.WriteObject(anchorEntry.Value);
                }
            }
            else
            {
                output.WriteInt(0);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        public void ReadData(IObjectDataInput input)
        {
            _predicate = input.ReadObject<IPredicate<TKey, TValue>>();
            _comparer = input.ReadObject<IComparer<KeyValuePair<TKey, TValue>>>();
            _page = input.ReadInt();
            _pageSize = input.ReadInt();
            _iterationType = (IterationType)Enum.Parse(typeof(IterationType), input.ReadUTF(), true);
            int size = input.ReadInt();
            _anchorList = new List<KeyValuePair<int, KeyValuePair<TKey, TValue>>> (size);
            for (int i = 0; i < size; i++)
            {
                int anchorPage = input.ReadInt();
                KeyValuePair<TKey, TValue> anchorEntry = new KeyValuePair<TKey, TValue>(input.ReadObject<TKey>(), input.ReadObject<TValue>());
                _anchorList.Add(new KeyValuePair<int, KeyValuePair<TKey, TValue>>(anchorPage, anchorEntry));
            }
        }

        public int GetFactoryId()
        {
            return FactoryIds.PredicateFactoryId;
        }

        public int GetId()
        {
            return PredicateDataSerializerHook.PagingPredicate;
        }

    }
}