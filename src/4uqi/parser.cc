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

ups_status_t
Parser::parse_select(const char *query, SelectStatement &stmt)
{
  using qi::int_;
  using qi::short_;
  using qi::lit;
  using qi::lexeme;
  using qi::alnum;
  using ascii::char_;
  using boost::spirit::qi::_1;
  using boost::spirit::qi::phrase_parse;
  using boost::spirit::ascii::space;
  using boost::spirit::ascii::string;
  using boost::spirit::ascii::no_case;
  using boost::phoenix::ref;

  char const *first = query;
  const char *last = first + std::strlen(first);

  qi::rule<const char *, std::string(), ascii::space_type> quoted_string;
  qi::rule<const char *, std::string(), ascii::space_type> unquoted_string;
  qi::rule<const char *, SelectStatement(), ascii::space_type> start;

  quoted_string %= lexeme['"' >> +(char_ - '"') >> '"'];
  unquoted_string %= lexeme [ +alnum ];

  start %=
      -no_case[lit("distinct")] [ref(stmt.distinct) = true]
      >> (
          unquoted_string [ref(stmt.function.first) = _1]
          | quoted_string [ref(stmt.function.first) = _1]
         )
      >> '(' >> lit("$key") >> ')'
      >> no_case[lit("from")] >> no_case[lit("database")]
      >> short_ [ref(stmt.dbid) = _1]
      >> -(no_case[lit("where")] >> (
          unquoted_string [ref(stmt.predicate.first) = _1]
          | quoted_string [ref(stmt.predicate.first) = _1]
         ) >> '(' >> lit("$key") >> ')')
      >> -(no_case[lit("limit")] >> int_ [ref(stmt.limit) = _1])
      >> -char_(';')
      ;

  bool r = phrase_parse(first, last, start, space);
  if (!r || first != last)
    return (UPS_PARSER_ERROR);

  ups_status_t st;

  // Split |function|; delimiter character is '@' (optional). The function
  // name is reduced to lower-case, and the plugin is loaded. If a library
  // name is specified then loading the plugin MUST succeed. If not then it
  // can fail - then most likely a builtin function was specified.
  size_t delim = stmt.function.first.find('@');
  if (delim != std::string::npos) {
    stmt.function.second = stmt.function.first.data() + delim + 1;
    stmt.function.first = stmt.function.first.substr(0, delim);
    boost::algorithm::to_lower(stmt.function.first);
    if ((st = PluginManager::import(stmt.function.second.c_str(),
                                stmt.function.first.c_str())))
      return (st);
  }
  else {
    boost::algorithm::to_lower(stmt.function.first);
    stmt.function_plg = PluginManager::get(stmt.function.first.c_str());
  }

  // the predicate is formatted in the same way, but is completeley optional
  if (!stmt.predicate.first.empty()) {
    delim = stmt.predicate.first.find('@');
    if (delim != std::string::npos) {
      stmt.predicate.second = stmt.predicate.first.data() + delim + 1;
      stmt.predicate.first = stmt.predicate.first.substr(0, delim);
      boost::algorithm::to_lower(stmt.predicate.first);
      if ((st = PluginManager::import(stmt.function.second.c_str(),
                                  stmt.function.first.c_str())))
        return (st);
    }
    else {
      boost::algorithm::to_lower(stmt.predicate.first);
      stmt.predicate_plg = PluginManager::get(stmt.predicate.first.c_str());
    }
  }

  return (0);
}

