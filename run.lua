--[[
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
]]--

local socket = require 'socket'

math.randomseed( socket.gettime() * 1000 )

local function sleep( secs )
	return socket.select(nil,nil,secs)
end

local lcbase = require 'libcbase'


local function dump( itm )
	print("Type ", type(itm))
	if type(itm) == 'table' then
		for i,v in pairs(itm) do
			print("  k: " .. tostring(i) .. " v: " .. tostring(v))
		end
	else
		print("  " .. tostring(itm))
	end
end

local function coWaitRes( conn, resD )
  local ok, err

  while not resD:isFinished() do
    ok, err = lcbase.wait( conn )
    if not ok then
      print("Error waiting ", err )
      break
    end

    --    sleep( .01 ) -- CMH coroutine yield here
  end

  return ok, err
end


print("Ok got something")

local function dumpTab( tab )
  if type(tab) == 'userdata' then
    print(" User data ")
    return
  end
  for i, v in pairs( tab ) do
	  print(i,v)
  end
end

dumpTab( lcbase )

local ok, err, conn, reqd

conn,err = lcbase.connect( '127.0.0.1:8091', 'default', 'admin', 'admin', true )
if not conn then
	print("Connection error ", err)
	os.exit(1)
end

print("Connected ", tostring(conn))

dumpTab( conn )


local keys = { 'hello', 'hello1', 'how', 'are', 'you' }
print("Get keys {" .. table.concat(keys, ", ") .. "}")
local reqD, err  = lcbase.get( conn, unpack(keys))
if not reqD then
	print("Failure creating get request err: " .. err)
end

local existing = {}

local function iterate( k, d, f, cas, err, fetched )
	print( "Key: [" .. tostring(k)
			.. "] Data: [" .. tostring(d)
			.. "] Flags: [" .. tostring(f)
			.. "] Cas [" .. cas
			.. "] Err [" .. tostring(err)
			.. "] Fetched? [" .. tostring(fetched) .. "]")
	if fetched and cas ~= 0 then
		existing[k] = { d, f, cas }
	end
end

if not coWaitRes( conn, reqD ) then
  os.exit(1)
end

print("Checking reqD")
print(" isFinished? [" .. tostring(reqD.isFinished( reqD ))	.. "]")
print(" keyCount?   [" .. tostring(reqD:keyCount())			.. "]")
print(" errorStr?   [" .. tostring(reqD:errorStr())			.. "]")

reqD:iterateRes( iterate )

print("Doing some stores")

local bStore = {}

local function storeCmd( lst, op, key, val, flags, cas, tm )
	table.insert(lst,op); print(op);
	table.insert(lst,key); print(key);
	table.insert(lst,val); print(val);
	table.insert(lst,flags or 0); print(flags or 0);
	table.insert(lst,cas or 0); print(cas or 0);
	table.insert(lst,tm or 0); print(tm or 0);
  print("----")
end

if existing['hello'][1] ~= nil then
	local d = 'cas_olleh'
	if d == existing['hello'][1] then
		d = 'cas_hello' .. math.random(1,50)
	end
	storeCmd( bStore, 'cas', 'hello', d, 0, existing['hello'][3] )
else
	storeCmd( bStore, 'add', 'hello', 'hello' )
end

storeCmd( bStore, 'set', 'hello1', 'this is hello 1s value' )

if existing['how'][1] ~= nil then
	local d = 'cas_woh'
	if d == existing['how'][1] then
		d = 'cas_how' .. math.random(1,30000)
	end
	storeCmd( bStore, 'cas', 'how', d, 0, existing['how'][3] )
else
	storeCmd( bStore, 'add', 'how', 'HOW ARE YOU' )
end

if existing['are'][1] ~= nil then
	local d = 'cas_era'
	if d == existing['are'][1] then
		d = 'cas_are' .. math.random(1,50)
	end
	storeCmd( bStore, 'cas', 'are', d, 0, existing['are'][3] )
else
	storeCmd( bStore, 'add', 'are', 'are' )
end

reqD, err = lcbase.store( conn, unpack(bStore) )
if not reqD then
	print("Failure issuing store, " .. err)
	os.exit(1)
end

local function iterStore( k, cas, op, err, fetched )
	print( "Key: [" .. tostring(k)
			.. "] Cas: [" .. tostring(cas)
			.. "] Err: [" .. tostring(err)
			.. "] Fetched: [" .. tostring(fetched) .. "]" )
end

print("Waiting")
if not coWaitRes( conn, reqD ) then
  os.exit(1)
end


print("Did store work?")
reqD:iterateRes( iterStore )

print("Incr/Decr?")

local function makeArith( lst, key, delta, timeout, inital, create )
	table.insert( lst, key )
	table.insert( lst, delta )
	table.insert( lst, timeout or 0 )
	table.insert( lst, initial or 0 )
	table.insert( lst, create or 0 )
end

local acmds = {}

makeArith( acmds, 'incrfail', 1, 0, 0, 0 )
makeArith( acmds, 'incrsuccess', 1, 0, 0, 1 )

reqD, err = lcbase.arith( conn, unpack( acmds ) )
if not reqD then
	print("Error arith " .. tostring(err))
	os.exit(1)
end
print("Waiting")
if not coWaitRes( conn, reqD ) then
  os.exit(1)
end

local ares = {}
local function iterArith( k, val, cas, err, fetched )
	print( "Key: [" .. tostring(k)
			.. "] Val: [" .. tostring(val)
			.. "] Cas: [" .. tostring(cas)
			.. "] Err: [" .. tostring(err)
			.. "] Fetched: [" .. tostring(fetched) .. "]" )
	if fetched and #err == 0 then
		ares[k] = { val, cas }
	end
end

reqD:iterateRes( iterArith )

if ares['incrsuccess'] ~= nil and ares['incrsuccess'][1] > 5 then
	acmds = {}
	makeArith( acmds, 'incrsuccess', -5, 0, 0, 1 )
	reqD, err = lcbase.arith( conn, unpack( acmds ) )
	if not reqD then
		print("Error arith " .. tostring(err))
		os.exit(1)
	end
  print("Waiting")
  if not coWaitRes( conn, reqD ) then
    os.exit(1)
  end
	reqD:iterateRes( iterArith )
end

print("Removing incrsuccess, hello")
local rcmds = {}
local function makeRemove( lst, key, cas )
	table.insert( lst, key )
	table.insert( lst, cas or 0 )
end

if ares['incrsuccess'] ~= nil and ares['incrsuccess'][1] > 2 and math.random(1,2) == 1 then
	makeRemove( rcmds, 'incrsuccess', ares['incrsuccess'][2] )
end
makeRemove( rcmds, 'you' )

reqD,err = lcbase.remove( conn, unpack( rcmds ) )
print("Waiting .. remove ..")
if not coWaitRes( conn, reqD ) then
  os.exit(1)
end

local function iterRem( k, cas, err, fetched )
	print( "Removed Key: [" .. tostring(k)
			.. "] Cas: [" .. tostring(cas)
			.. "] Err: [" .. tostring(err)
			.. "] Fetched: [" .. tostring(fetched) .. "]" )
end
reqD:iterateRes( iterRem )

if math.random(1,100) < 0 then
	print("Flush everything")
	resD, err = lcbase.flush( conn )
	if not resD then
		print("Error flushing db err: " .. err)
		os.exit(1)
	end
  if not coWaitRes( conn, reqD ) then
    os.exit(1)
  end

	if reqD:isFinished() then
		print("Ok db cleared with err " .. tostring(reqD:errorStr()))
	end
end

print("Test expire")

bStore = {}
storeCmd( bStore, "add", "unq", "super unique", 0, 0, 1 )

reqD, err = lcbase.store( conn, unpack(bStore) )
if not reqD then
	print("Failure issuing store, " .. err)
	os.exit(1)
end

print("Waiting .. add ..")
if not coWaitRes( conn, reqD ) then
  os.exit(1)
end

reqD:iterateRes( iterStore )

local done = false

while not done do
	print("Sleep for .5 sec")
	sleep(.5)
	reqD, err  = lcbase.get( conn, 'unq')
	if not reqD then
		print("Error waiting for get ops: " .. err)
		os.exit(1)
	end
	
  print("Waiting .. expire ..")
  if not coWaitRes( conn, reqD ) then
    os.exit(1)
  end
	
	local function getD( k, d, f, cas, err, fetched )
		if k == 'unq' then
			if d ~= nil and #d > 0 then
				print("Still there ", d )
			else
				print("Its gone")
				done = true
			end
		end
	end
	reqD:iterateRes( getD )
end

print("Done")
