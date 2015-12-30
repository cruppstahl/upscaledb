/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

#include "0root/root.h"

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix.hpp>
#include <boost/spirit/include/qi_no_case.hpp>
#include <boost/bind.hpp>
#include <boost/algorithm/string.hpp>    

#include "4uqi/parser.h"
#include "4uqi/plugins.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

using namespace upscaledb;

namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;

static bool initialized = false;
static qi::rule<const char *, std::string(), ascii::space_type> quoted_string;
static qi::rule<const char *, std::string(), ascii::space_type> unquoted_string;
static qi::rule<const char *, std::string(), ascii::space_type> plugin_name;
static qi::rule<const char *, std::string(), ascii::space_type> where_clause;
static qi::rule<const char *, int(), ascii::space_type> limit_clause;
static qi::rule<const char *, short(), ascii::space_type> from_clause;
static qi::rule<const char *, short(), ascii::space_type> number;
static qi::rule<const char *, int(), ascii::space_type> input_clause;

static void
initialize_parsers()
{
  using qi::lexeme;
  using qi::alnum;
  using qi::int_;
  using qi::short_;
  using qi::lit;
  using qi::_val;
  using ascii::char_;
  using ascii::no_case;

  quoted_string %= lexeme['"' >> +(char_ - '"') >> '"'][_val];
  unquoted_string %= lexeme[ +(alnum | char_("-_"))][_val];
  plugin_name %= unquoted_string | quoted_string;
  where_clause = no_case[lit("where")] >> plugin_name;
  limit_clause = no_case[lit("limit")] >> int_;
  from_clause = no_case[lit("from")] >> no_case[lit("database")]
                    >> number;
  number = (no_case[lit("0x")] >> boost::spirit::hex)
           | ('0' >> boost::spirit::oct)
           | short_
      ;
  input_clause =
        (lit("$key") >> ',' >> lit("$record"))[_val = UQI_STREAM_KEY | UQI_STREAM_RECORD]
        | lit("$key")[_val = UQI_STREAM_KEY]
        | lit("$record")[_val = UQI_STREAM_RECORD]
      ;
}

ups_status_t
Parser::parse_select(const char *query, SelectStatement &stmt)
{
  using qi::int_;
  using qi::lexeme;
  using qi::alnum;
  using qi::lit;
  using ascii::char_;
  using ascii::no_case;
  using boost::spirit::qi::_1;
  using boost::spirit::qi::phrase_parse;
  using boost::spirit::ascii::space;
  using boost::spirit::ascii::string;
  using boost::phoenix::ref;

  if (!initialized) {
    initialized = true;
    initialize_parsers();
  }

  char const *first = query;
  const char *last = first + std::strlen(first);

  qi::rule<const char *, SelectStatement(), ascii::space_type> parser;

  stmt.function.flags = 0;
  stmt.predicate.flags = 0;

  parser %=
      -no_case[lit("distinct")] [ref(stmt.distinct) = true]
      >> plugin_name [ref(stmt.function.name) = _1]
        >> '(' >> input_clause [ref(stmt.function.flags) = _1] >> ')'
      >> from_clause [ref(stmt.dbid) = _1]
      >> -(where_clause [ref(stmt.predicate.name) = _1]
        >> '(' >> input_clause [ref(stmt.predicate.flags) = _1] >> ')')
      >> -limit_clause [ref(stmt.limit) = _1]
      >> -char_(';')
      ;

  bool r = phrase_parse(first, last, parser, space);
  if (!r || first != last)
    return (UPS_PARSER_ERROR);

  ups_status_t st;

  // Split |function|; delimiter character is '@' (optional). The function
  // name is reduced to lower-case, and the plugin is loaded. If a library
  // name is specified then loading the plugin MUST succeed. If not then it
  // can fail - then most likely a builtin function was specified.
  size_t delim = stmt.function.name.find('@');
  if (delim != std::string::npos) {
    stmt.function.library = stmt.function.name.data() + delim + 1;
    stmt.function.name = stmt.function.name.substr(0, delim);
    boost::algorithm::to_lower(stmt.function.name);
    if ((st = PluginManager::import(stmt.function.library.c_str(),
                                stmt.function.name.c_str())))
      return (st);
  }
  else {
    boost::algorithm::to_lower(stmt.function.name);
    stmt.function_plg = PluginManager::get(stmt.function.name.c_str());
  }

  // the predicate is formatted in the same way, but is completeley optional
  if (!stmt.predicate.name.empty()) {
    delim = stmt.predicate.name.find('@');
    if (delim != std::string::npos) {
      stmt.predicate.library = stmt.predicate.name.data() + delim + 1;
      stmt.predicate.name = stmt.predicate.name.substr(0, delim);
      boost::algorithm::to_lower(stmt.predicate.name);
      if ((st = PluginManager::import(stmt.function.library.c_str(),
                                  stmt.function.name.c_str())))
        return (st);
    }
    else {
      boost::algorithm::to_lower(stmt.predicate.name);
      stmt.predicate_plg = PluginManager::get(stmt.predicate.name.c_str());
    }
  }

  return (0);
}

