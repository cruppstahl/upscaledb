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

#include "5upscaledb/parser.h"
#include "5upscaledb/plugins.h"

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

  // split |function|; delimiter character is '@' (optional)
  size_t delim = stmt.function.first.find('@');
  if (delim != std::string::npos) {
    stmt.function.second = stmt.function.first.data() + delim + 1;
    stmt.function.first = stmt.function.first.substr(0, delim);
    if ((st = PluginManager::import(stmt.function.second.c_str(),
                                stmt.function.first.c_str())))
      return (st);
  }
  else if (!PluginManager::is_registered(stmt.function.first.c_str()))
    return (UPS_PLUGIN_NOT_FOUND);

  // the predicate is formatted in the same way, but is completeley optional
  if (!stmt.predicate.first.empty()) {
    delim = stmt.predicate.first.find('@');
    if (delim != std::string::npos) {
      stmt.predicate.second = stmt.predicate.first.data() + delim + 1;
      stmt.predicate.first = stmt.predicate.first.substr(0, delim);
      if ((st = PluginManager::import(stmt.function.second.c_str(),
                                  stmt.function.first.c_str())))
        return (st);
    }
    else if (!PluginManager::is_registered(stmt.predicate.first.c_str()))
      return (UPS_PLUGIN_NOT_FOUND);
  }

  return (0);
}

