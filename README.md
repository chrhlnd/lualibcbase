Copyright 2013 Chris Holland

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

This is a binding from libcouchbase to lua. Its not fully implemented I'll get around to adding the TOUCH and STAT command some day. Also it is using malloc/strdup/free in some places. This could probably be changed to use something less fragmenty or just pushed data right into the luaState. Its doing its job at the moment though.

The run.lua and runsync.lua give an example of usage. Where I actually use this I'm using it in place of a memcache layer, so I wrote a binding to this that looked like memcache as a drop in.

To Compile it.

Simple Makefile at the moment for Linux/Mac
	Adjust this file to your needs. I was compiling for a webapp server. You'll probably want to adjust your include paths. Make sure you have libcouchbase2 libevent1.4 installed

To use it

1) Connect -> connection

2) Issue Command (STORE,GET,ARITH,DELETE) -> Pending Response

3) Wait() -> ok, error

4) Check Pending Response 'isFinished'

5) If isFinished call 'iterateRes' on Pending Response

6) Callback passed to iterateRes will be called with params appropriate to the command type


You can run it in SYNC or ASYNC mode. In SYNC mode Wait blocks until all Pending Responses have been answered. In ASYNC mode wait will return immediatly if there is no data. If there is some data it may fill in some Pending Response objects and not others. The idea here is you could check your Pending Response objects by calling isFinished, if not then do something like coroutine.yield or sleep or somehow share processing time, then call wait again and check responses again.

All commands can be batched, just issue multiple full sets of parameters to the call

IE
```lua
	libcbase.store( conn
					, 'cas',  key1, value1, flags1, cas1, timeout1
					, 'set', key2, value2, flags2, cas2, timeout2
					, 'add', key3, value3, flags3, cas3, timeout3
					,....
					)
```

The response object returned will responsed with each key's response data through iterate.

Things need to do to be proper:
 - [ ]	Finish implementing TOUCH/STATS maybe look at the rest api stuff
 - [ ]	Make proper build path
 - [ ]	Adjust allocation strategy

