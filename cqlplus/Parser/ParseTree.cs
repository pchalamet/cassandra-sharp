// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2018 Pierre Chalamet
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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using cqlplus.Commands;

namespace cqlplus.Parser
{
    #region ParseTree

    [Serializable]
    public class ParseErrors : List<ParseError>
    {
    }

    [Serializable]
    public class ParseError
    {
        public ParseError()
        {
        }

        public ParseError(string message, int code, ParseNode node)
            : this(message, code, 0, node.Token.StartPos, node.Token.StartPos, node.Token.Length)
        {
        }

        public ParseError(string message, int code, int line, int col, int pos, int length)
        {
            Message = message;
            Code = code;
            Line = line;
            Column = col;
            Position = pos;
            Length = length;
        }

        public int Code { get; }

        public int Line { get; }

        public int Column { get; }

        public int Position { get; }

        public int Length { get; }

        public string Message { get; }

        // just for the sake of serialization
    }

    // rootlevel of the node tree
    [Serializable]
    public class ParseTree : ParseNode
    {
        public ParseErrors Errors;

        public List<Token> Skipped;

        public ParseTree()
            : base(new Token(), "ParseTree")
        {
            Token.Type = TokenType.Start;
            Token.Text = "Root";
            Errors = new ParseErrors();
        }

        public string PrintTree()
        {
            var sb = new StringBuilder();
            var indent = 0;
            PrintNode(sb, this, indent);
            return sb.ToString();
        }

        private void PrintNode(StringBuilder sb, ParseNode node, int indent)
        {
            var space = "".PadLeft(indent, ' ');

            sb.Append(space);
            sb.AppendLine(node.Text);

            foreach (var n in node.Nodes) PrintNode(sb, n, indent + 2);
        }

        /// <summary>
        ///     this is the entry point for executing and evaluating the parse tree.
        /// </summary>
        /// <param name="paramlist">additional optional input parameters</param>
        /// <returns>the output of the evaluation function</returns>
        public object Eval(params object[] paramlist)
        {
            return Nodes[0].Eval(this, paramlist);
        }
    }

    [Serializable]
    [XmlInclude(typeof(ParseTree))]
    public class ParseNode
    {
        protected List<ParseNode> nodes;

        [XmlIgnore] // avoid circular references when serializing
        public ParseNode Parent;

        protected string text;

        public Token Token; // the token/rule

        protected ParseNode(Token token, string text)
        {
            Token = token;
            this.text = text;
            nodes = new List<ParseNode>();
        }

        public List<ParseNode> Nodes => nodes;

        [XmlIgnore] // skip redundant text (is part of Token)
        public string Text
        {
            // text to display in parse tree 
            get => text;
            set => text = value;
        }

        public virtual ParseNode CreateNode(Token token, string text)
        {
            var node = new ParseNode(token, text);
            node.Parent = this;
            return node;
        }

        protected object GetValue(ParseTree tree, TokenType type, int index)
        {
            return GetValue(tree, type, ref index);
        }

        protected object GetValue(ParseTree tree, TokenType type, ref int index)
        {
            object o = null;
            if (index < 0) return o;

            // left to right
            foreach (var node in nodes)
                if (node.Token.Type == type)
                {
                    index--;
                    if (index < 0)
                    {
                        o = node.Eval(tree);
                        break;
                    }
                }

            return o;
        }

        internal object Default(params object[] values)
        {
            return values.FirstOrDefault(x => null != x);
        }

        /// <summary>
        ///     this implements the evaluation functionality, cannot be used directly
        /// </summary>
        /// <param name="tree">the parsetree itself</param>
        /// <param name="paramlist">optional input parameters</param>
        /// <returns>a partial result of the evaluation</returns>
        internal object Eval(ParseTree tree, params object[] paramlist)
        {
            object Value = null;

            switch (Token.Type)
            {
                case TokenType.String:
                    Value = EvalString(tree, paramlist);
                    break;
                case TokenType.Identifier:
                    Value = EvalIdentifier(tree, paramlist);
                    break;
                case TokenType.Integer:
                    Value = EvalInteger(tree, paramlist);
                    break;
                case TokenType.Value:
                    Value = EvalValue(tree, paramlist);
                    break;
                case TokenType.Parameters:
                    Value = EvalParameters(tree, paramlist);
                    break;
                case TokenType.CommandWithParameters:
                    Value = EvalCommandWithParameters(tree, paramlist);
                    break;
                case TokenType.CqlCommand:
                    Value = EvalCqlCommand(tree, paramlist);
                    break;
                case TokenType.Start:
                    Value = EvalStart(tree, paramlist);
                    break;

                default:
                    Value = Token.Text;
                    break;
            }

            return Value;
        }

        protected virtual object EvalString(ParseTree tree, params object[] paramlist)
        {
            var str = (string)GetValue(tree, TokenType.STRING, 0);
            str = str.Substring(1, str.Length - 2);
            return str;
        }

        protected virtual object EvalIdentifier(ParseTree tree, params object[] paramlist)
        {
            return GetValue(tree, TokenType.IDENTIFIER, 0);
        }

        protected virtual object EvalInteger(ParseTree tree, params object[] paramlist)
        {
            return GetValue(tree, TokenType.INTEGER, 0);
        }

        protected virtual object EvalValue(ParseTree tree, params object[] paramlist)
        {
            return Default(GetValue(tree, TokenType.String, 0), GetValue(tree, TokenType.Identifier, 0), GetValue(tree, TokenType.Integer, 0));
        }

        protected virtual object EvalParameters(ParseTree tree, params object[] paramlist)
        {
            var res = new Dictionary<string, string>();
            for (var i = 0; GetValue(tree, TokenType.Identifier, i) != null; ++i)
            {
                var prmName = ((string)GetValue(tree, TokenType.Identifier, i)).ToLower();
                var prmValue = (string)GetValue(tree, TokenType.Value, i);
                res[prmName] = prmValue;
            }

            return res;
        }

        protected virtual object EvalCommandWithParameters(ParseTree tree, params object[] paramlist)
        {
            return new ShellCommand((string)GetValue(tree, TokenType.Identifier, 0), (Dictionary<string, string>)GetValue(tree, TokenType.Parameters, 0));
        }

        protected virtual object EvalCqlCommand(ParseTree tree, params object[] paramlist)
        {
            return new CqlStatement((string)GetValue(tree, TokenType.EVERYTHING_BUT_START_WITH_BANG, 0));
        }

        protected virtual object EvalStart(ParseTree tree, params object[] paramlist)
        {
            return Default(GetValue(tree, TokenType.CommandWithParameters, 0), GetValue(tree, TokenType.CqlCommand, 0));
        }
    }

    #endregion ParseTree
}