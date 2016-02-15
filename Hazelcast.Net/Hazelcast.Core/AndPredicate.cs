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

namespace Hazelcast.Core
{
    /// <summary>
    /// And Predicate
    /// </summary>
    public class AndPredicate<TKey, TValue> : IPredicate<TKey, TValue>
    {
        private IPredicate<TKey, TValue>[] _predicates;

        public AndPredicate()
        {
        }

        public AndPredicate(IPredicate<TKey, TValue>[] predicates)
        {
            _predicates = predicates;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="output"></param>
        public void WriteData(IObjectDataOutput output)
        {
            output.WriteInt(_predicates.Length);
            foreach (IPredicate<TKey, TValue> predicate in _predicates)
            {
                output.WriteObject(predicate);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        public void ReadData(IObjectDataInput input)
        {
            int size = input.ReadInt();
            _predicates = new IPredicate<TKey, TValue>[size];
            for (int i = 0; i < size; ++i)
            {
                _predicates[i] = input.ReadObject<IPredicate<TKey, TValue>>();
            }    
        }

        public int GetFactoryId()
        {
            return FactoryIds.PredicateFactoryId;
        }

        public int GetId()
        {
            return PredicateDataSerializerHook.AndPredicate;
        }
    }
}