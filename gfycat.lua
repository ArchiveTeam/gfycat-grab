local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false
local is_initial_url = true
local has_size_restricted = false
local has_mp4 = false
local has_mobile_mp4 = false
local views = 0

abort_item = function(item)
  abortgrab = true
  killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
    local a, b = string.match(item, "^([^:]+):(.+)$")
    if a and b and a == "post" then
      discover_item(target, "post-api:" .. b)
    end
--print('discovered', item)
    target[item] = true
    return true
  end
  return false
end

find_item = function(url)
  local value = string.match(url, "^https?://api%.gfycat%.com/v1/gfycats/([a-zA-Z0-9]+)$")
  if value then
    value = string.lower(value)
  end
  local type_ = "gif"
  if not value then
    value = string.match(url, "^https?://api%.gfycat%.com/v1/users/([a-zA-Z0-9]+)$")
    type_ = "user"
  end
  if value then
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    item_type = found["type"]
    item_value = found["value"]
    item_name_new = item_type .. ":" .. item_value
    if item_name_new ~= item_name then
      ids = {}
      ids[item_value] = true
      abortgrab = false
      tries = 0
      retry_url = false
      has_size_restricted = false
      has_mp4 = false
      has_mobile_mp4 = false
      views = 0
      is_initial_url = true
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

allowed = function(url, parenturl)
  if ids[url]
    or ids[string.match(url, "^https?://(.*)$")] then
    return true
  end

  if string.match(url, "/undefined$")
    or string.match(url, "^https?://[^/]+/ifr/.*collections/")
    or string.match(url, "^https?://[^/]+/uk/")
    or string.match(url, "^https?://[^/]+/ru/")
    or string.match(url, "^https?://[^/]+/pl/")
    or string.match(url, "^https?://[^/]+/ko/")
    or string.match(url, "^https?://[^/]+/fr/")
    or string.match(url, "^https?://pinterest%.com/pin/create/button/%?url=")
    or string.match(url, "^https?://vk%.com/share%.php%?url=")
    or string.match(url, "^https?://www%.facebook%.com/dialog/share%?u=")
    or string.match(url, "^https?://twitter%.com/intent/tweet%?url=")
    or string.match(url, "^https?://www%.reddit%.com/submit%?url=")
    or string.match(url, "^https?://www%.tumblr%.com/share/link%?url=")
    or string.match(url, "^https?://metrics%.gfycat%.com/pix%.gif%?")
    or (
      (has_size_restricted and has_mp4)
      and (
        string.match(url, "^https?://[^/]+/[a-zA-Z]+%.gif$")
        or string.match(url, "^https?://[^/]+/[a-zA-Z]+%.webm$")
        or string.match(url, "^https?://[^/]+/[a-zA-Z]+%.webp$")
      )
    )
    or (
      has_mobile_mp4
      and has_mp4
      --and views < 3000
      and string.match(url, "/[a-zA-Z]+%.mp4$")
    )
    or (
      has_size_restricted
      and (
        string.match(url, "%-max%-1mb%.gif$")
        or string.match(url, "%-small%.gif$")
        or string.match(url, "%-100px%.gif$")
      )
    ) then
    return false
  end

  local found = false
  for pattern, type_ in pairs({
    ["^https?://www%.gfycat%.com/([a-zA-Z0-9]+)[^/]-$"]="gif",
    ["^https?://gfycat%.com/([a-zA-Z0-9]+)[^/]-$"]="gif"
  }) do
    match = string.match(url, pattern)
    if match then
      if type_ == "gif" then
        match = string.lower(match)
      end
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name then
        discover_item(discovered_items, new_item)
        return false
      end
    end
  end

  if string.match(url, "^https?://[^/]*gfycat%.com/") then
    for _, pattern in pairs({
      "([a-zA-Z0-9]+)",
      "([^/%?&]+)"
    }) do
      for s in string.gmatch(string.match(url, "^https?://[^/]+(/.*)"), pattern) do
        if ids[string.lower(s)] then
          return true
        end
      end
    end
  end

  if not string.match(url, "^https?://[^/]*gfycat%.com/") then
    discover_item(discovered_outlinks, url)
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if allowed(url, parent["url"]) and (
    not processed(url)
  ) and string.match(url, "^https://") and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  --[[local function decode_codepoint(newurl)
    newurl = string.gsub(
      newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return unicode_codepoint_as_utf8(tonumber(s, 16))
      end
    )
    return newurl
  end]]

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    if not newurl then
      newurl = ""
    end
    --newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0 then
      return nil
    end
    local a, b = string.match(newurl, "^(https?)(:.+)$")
    if a == "http" then
      newurl = "https" .. b
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      table.insert(urls, {
        url=url_
      })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    --newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    --newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function check_new_params(newurl, param, value)
    if string.match(newurl, "[%?&]" .. param .. "=") then
      newurl = string.gsub(newurl, "([%?&]" .. param .. "=)[^%?&;]+", "%1" .. value)
    else
      if string.match(newurl, "%?") then
        newurl = newurl .. "&"
      else
        newurl = newurl .. "?"
      end
      newurl = newurl .. param .. "=" .. value
    end
    check(newurl)
  end

  local function flatten_json(json)
    local result = ""
    for k, v in pairs(json) do
      result = result .. " " .. k
      local type_v = type(v)
      if type_v == "string" then
        v = string.gsub(v, "\\", "")
        result = result .. " " .. v .. ' "' .. v .. '"'
      elseif type_v == "table" then
        result = result .. " " .. flatten_json(v)
      end
    end
    return result
  end

  local function extract_from_json(json)
    for k, v in pairs(json) do
      if type(v) == "table" then
        extract_from_json(v)
      elseif type(v) == "string" then
        if k == "username" then
          discover_item(discovered_items, "user:" .. v)
        elseif k == "gfyId" then
          discover_item(discovered_items, "gif:" .. v)
        end
      end
    end
  end

  if allowed(url)
    and status_code < 300
    and not string.match(url, "^https?://thumbs%.gfycat%.com/")
    and not string.match(url, "^https?://giant%.gfycat%.com/")
    and not string.match(url, "^https?://fat%.gfycat%.com/")
    and not string.match(url, "^https?://zippy%.gfycat%.com/") then
    html = read_file(file)
    if string.match(url, "^https?://api%.gfycat%.com/") then
      local json = cjson.decode(html)
      extract_from_json(json)
      if string.match(url, "/v1/gfycats/") then
        if item_value ~= string.lower(json["gfyItem"]["gfyId"]) then
          error("Wrong gfyId.")
        end
        for _, tag in pairs(json["gfyItem"]["tags"]) do
          discover_item(discovered_items, "search:" .. tag)
        end
        for _, category in pairs(json["gfyItem"]["languageCategories"]) do
          discover_item(discovered_items, "discover:" .. category)
        end
        check("https://gfycat.com/" .. item_value)
        if string.match(html, '%-size_restricted%.gif"') then
          has_size_restricted = true
        end
        if string.match(html, '/[a-zA-Z]+%.mp4"') then
          has_mp4 = true
        end
        if string.match(html, '%-mobile%.mp4"') then
          has_mobile_mp4 = true
        end
        views = json["gfyItem"]["views"]
        if not views then
          views = 0
        end
        --[[if views < 3000 then
          abort_item()
          return {}
        end]]
      elseif string.match(url, "/v1/users/") then
        if string.match(url, "/collections[^/]*$") then
          for _, collection in pairs(json["gfyCollections"]) do
            check(urlparse.absolute(url, "collections/" .. collection["folderId"] .. "/gfycats?count=30"))
            check(urlparse.absolute(url, "collections/" .. collection["folderId"] .. "/gfycats"))
            if json["tags"] then
              for _, tag in pairs(json["tags"]) do
                discover_item(discovered_items, "search:" .. tag)
              end
            end
          end
        end
        local image_url = json["profileImageUrl"]
        if image_url then
          ids[image_url] = true
          check(image_url)
        end
        for _, endpoint in pairs({
          "collections",
          "likes",
          "likes/populated",
          "gfycats"
        }) do
          for _, params in pairs({
            "",
            "?count=30"
          }) do
            check("https://api.gfycat.com/v1/users/" .. item_value .. "/" .. endpoint .. params)
          end
        end
      end
      if json["cursor"] and string.len(json["cursor"]) > 0 then
        check_new_params(url, "cursor", json["cursor"])
      end
      html = html .. flatten_json(json)
    end
    html = string.gsub(html, "\\", "")
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if string.match(url["url"], "^https?://api%.gfycat%.com/") then
    local html = read_file(http_stat["local_file"])
    if not (string.match(html, "^%s*{") and string.match(html, "}%s*$"))
      and not (string.match(html, "^%s*%[") and string.match(html, "%]%s*$")) then
      print("Did not get JSON data.")
      retry_url = true
      return false
    end
    local json = cjson.decode(html)
    if json["status"] and json["status"] ~= "ok" then
      print("Problem with JSON.")
      retry_url = true
      return false
    end
  end
  if http_stat["statcode"] == 403 then
    local html = read_file(http_stat["local_file"])
    if not string.match(html, "<h1>403 ERROR</h1>")
      and not string.match(html, "Request blocked%.") then
      tries = 10
    end
    retry_url = true
    return false
  end
  if http_stat["statcode"] ~= 200
    and http_stat["statcode"] ~= 301 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if string.match(url["url"], "^https?://gfycat%.com/")
    or string.match(url["url"], "^https?://www%.gfycat%.com/")
    or string.match(url["url"], "^https?://api%.gfycat%.com/") then
    os.execute("sleep " .. tostring(0.5*concurrency))
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    if tries > 9 then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    downloaded[url["url"]] = true
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 10
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["gfycat2-q93gzeh1o8cs4mb3"] = discovered_items,
    ["urls-p4ku0cw5pzy9chv9"] = discovered_outlinks
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 100 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


