/*
* Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using Hazelcast.Core;

namespace Hazelcast.Transaction
{
    /// <summary>
    ///     A
    ///     <see cref="Hazelcast.Core.HazelcastException">Hazelcast.Core.HazelcastException</see>
    ///     thrown when an a transactional operation is executed without an active transaction.
    /// </summary>
    [Serializable]
    internal class TransactionNotActiveException : HazelcastException
    {
        public TransactionNotActiveException()
        {
        }

        public TransactionNotActiveException(string message) : base(message)
        {
        }
    }
}