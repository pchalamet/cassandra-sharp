// cassandra-sharp - the high performance .NET CQL 3 binary protocol client for Apache Cassandra
// Copyright (c) 2011-2013 Pierre Chalamet
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

namespace cqlplus.Parser
{
    using System.Collections.Generic;
    using System.Text.RegularExpressions;
    using System.Xml.Serialization;

    #region Scanner
    public class Scanner
    {
        private readonly List<TokenType> SkipList; // tokens to be skipped

        private readonly List<TokenType> Tokens;

        public int CurrentColumn;

        public int CurrentLine;

        public int CurrentPosition;

        public int EndPos = 0;

        public string Input;

        private Token LookAheadToken;

        public Dictionary<TokenType, Regex> Patterns;

        public List<Token> Skipped; // tokens that were skipped

        public int StartPos = 0;

        public Scanner()
        {
            Regex regex;
            Patterns = new Dictionary<TokenType, Regex>();
            Tokens = new List<TokenType>();
            LookAheadToken = null;
            Skipped = new List<Token>();

            SkipList = new List<TokenType>();
            SkipList.Add(TokenType.WHITESPACE);
            SkipList.Add(TokenType.COMMENT2);

            regex = new Regex(@"\s+", RegexOptions.Compiled);
            Patterns.Add(TokenType.WHITESPACE, regex);
            Tokens.Add(TokenType.WHITESPACE);

            regex = new Regex(@"/\*[^*]*\*+(?:[^/*][^*]*\*+)*/", RegexOptions.Compiled);
            Patterns.Add(TokenType.COMMENT2, regex);
            Tokens.Add(TokenType.COMMENT2);

            regex = new Regex(@"^$", RegexOptions.Compiled);
            Patterns.Add(TokenType.EOF, regex);
            Tokens.Add(TokenType.EOF);

            regex = new Regex(@"@?\""(\""\""|[^\""])*\""", RegexOptions.Compiled);
            Patterns.Add(TokenType.STRING, regex);
            Tokens.Add(TokenType.STRING);

            regex = new Regex(@"\b", RegexOptions.Compiled);
            Patterns.Add(TokenType.WORD, regex);
            Tokens.Add(TokenType.WORD);

            regex = new Regex(@"[+-]?[0-9]+", RegexOptions.Compiled);
            Patterns.Add(TokenType.INTEGER, regex);
            Tokens.Add(TokenType.INTEGER);

            regex = new Regex(@"[a-zA-Z_][a-zA-Z0-9_]*", RegexOptions.Compiled);
            Patterns.Add(TokenType.IDENTIFIER, regex);
            Tokens.Add(TokenType.IDENTIFIER);

            regex = new Regex("!", RegexOptions.Compiled);
            Patterns.Add(TokenType.BANG, regex);
            Tokens.Add(TokenType.BANG);

            regex = new Regex("[.]+", RegexOptions.Compiled);
            Patterns.Add(TokenType.EVERYTHING, regex);
            Tokens.Add(TokenType.EVERYTHING);

            regex = new Regex("[^!]+[.]*", RegexOptions.Compiled);
            Patterns.Add(TokenType.EVERYTHING_BUT_START_WITH_BANG, regex);
            Tokens.Add(TokenType.EVERYTHING_BUT_START_WITH_BANG);

            regex = new Regex("-", RegexOptions.Compiled);
            Patterns.Add(TokenType.MINUS, regex);
            Tokens.Add(TokenType.MINUS);

            regex = new Regex("=", RegexOptions.Compiled);
            Patterns.Add(TokenType.EQUAL, regex);
            Tokens.Add(TokenType.EQUAL);
        }

        public void Init(string input)
        {
            Input = input;
            StartPos = 0;
            EndPos = 0;
            CurrentLine = 0;
            CurrentColumn = 0;
            CurrentPosition = 0;
            LookAheadToken = null;
        }

        public Token GetToken(TokenType type)
        {
            Token t = new Token(StartPos, EndPos);
            t.Type = type;
            return t;
        }

        /// <summary>
        ///     executes a lookahead of the next token
        ///     and will advance the scan on the input string
        /// </summary>
        /// <returns></returns>
        public Token Scan(params TokenType[] expectedtokens)
        {
            Token tok = LookAhead(expectedtokens); // temporarely retrieve the lookahead
            LookAheadToken = null; // reset lookahead token, so scanning will continue
            StartPos = tok.EndPos;
            EndPos = tok.EndPos; // set the tokenizer to the new scan position
            return tok;
        }

        /// <summary>
        ///     returns token with longest best match
        /// </summary>
        /// <returns></returns>
        public Token LookAhead(params TokenType[] expectedtokens)
        {
            int i;
            int startpos = StartPos;
            Token tok = null;
            List<TokenType> scantokens;

            // this prevents double scanning and matching
            // increased performance
            if (LookAheadToken != null
                && LookAheadToken.Type != TokenType._UNDETERMINED_
                && LookAheadToken.Type != TokenType._NONE_)
            {
                return LookAheadToken;
            }

            // if no scantokens specified, then scan for all of them (= backward compatible)
            if (expectedtokens.Length == 0)
            {
                scantokens = Tokens;
            }
            else
            {
                scantokens = new List<TokenType>(expectedtokens);
                scantokens.AddRange(SkipList);
            }

            do
            {
                int len = -1;
                TokenType index = (TokenType) int.MaxValue;
                string input = Input.Substring(startpos);

                tok = new Token(startpos, EndPos);

                for (i = 0; i < scantokens.Count; i++)
                {
                    Regex r = Patterns[scantokens[i]];
                    Match m = r.Match(input);
                    if (m.Success && m.Index == 0 && ((m.Length > len) || (scantokens[i] < index && m.Length == len)))
                    {
                        len = m.Length;
                        index = scantokens[i];
                    }
                }

                if (index >= 0 && len >= 0)
                {
                    tok.EndPos = startpos + len;
                    tok.Text = Input.Substring(tok.StartPos, len);
                    tok.Type = index;
                }
                else if (tok.StartPos < tok.EndPos - 1)
                {
                    tok.Text = Input.Substring(tok.StartPos, 1);
                }

                if (SkipList.Contains(tok.Type))
                {
                    startpos = tok.EndPos;
                    Skipped.Add(tok);
                }
                else
                {
                    // only assign to non-skipped tokens
                    tok.Skipped = Skipped; // assign prior skips to this token
                    Skipped = new List<Token>(); //reset skips
                }
            } while (SkipList.Contains(tok.Type));

            LookAheadToken = tok;
            return tok;
        }
    }
    #endregion

    #region Token
    public enum TokenType
    {
        //Non terminal tokens:
        _NONE_ = 0,

        _UNDETERMINED_ = 1,

        //Non terminal tokens:
        String = 2,

        Identifier = 3,

        Integer = 4,

        Value = 5,

        Parameters = 6,

        CommandWithParameters = 7,

        CqlCommand = 8,

        Start = 9,

        //Terminal tokens:
        WHITESPACE = 10,

        COMMENT2 = 11,

        EOF = 12,

        STRING = 13,

        WORD = 14,

        INTEGER = 15,

        IDENTIFIER = 16,

        BANG = 17,

        EVERYTHING = 18,

        EVERYTHING_BUT_START_WITH_BANG = 19,

        MINUS = 20,

        EQUAL = 21
    }

    public class Token
    {
        [XmlAttribute]
        public TokenType Type;

        private int endpos;

        private int startpos;

        public Token()
                : this(0, 0)
        {
        }

        public Token(int start, int end)
        {
            Type = TokenType._UNDETERMINED_;
            startpos = start;
            endpos = end;
            Text = ""; // must initialize with empty string, may cause null reference exceptions otherwise
            Value = null;
        }

        // contains all prior skipped symbols

        public int StartPos
        {
            get { return startpos; }
            set { startpos = value; }
        }

        public int Length
        {
            get { return endpos - startpos; }
        }

        public int EndPos
        {
            get { return endpos; }
            set { endpos = value; }
        }

        public string Text { get; set; }

        public List<Token> Skipped { get; set; }

        public object Value { get; set; }

        public void UpdateRange(Token token)
        {
            if (token.StartPos < startpos)
            {
                startpos = token.StartPos;
            }
            if (token.EndPos > endpos)
            {
                endpos = token.EndPos;
            }
        }

        public override string ToString()
        {
            if (Text != null)
            {
                return Type.ToString() + " '" + Text + "'";
            }
            else
            {
                return Type.ToString();
            }
        }
    }
    #endregion
}