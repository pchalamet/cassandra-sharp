// cassandra-sharp - high performance .NET driver for Apache Cassandra
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


#pragma warning disable 168

namespace cqlplus.Parser
{

    #region Parser
    public class Parser
    {
        private readonly Scanner scanner;

        private ParseTree tree;

        public Parser(Scanner scanner)
        {
            this.scanner = scanner;
        }

        public ParseTree Parse(string input)
        {
            tree = new ParseTree();
            return Parse(input, tree);
        }

        public ParseTree Parse(string input, ParseTree tree)
        {
            scanner.Init(input);

            this.tree = tree;
            ParseStart(tree);
            tree.Skipped = scanner.Skipped;

            return tree;
        }

        private void ParseString(ParseNode parent)
        {
            Token tok;
            ParseNode n;
            ParseNode node = parent.CreateNode(scanner.GetToken(TokenType.String), "String");
            parent.Nodes.Add(node);

            tok = scanner.Scan(TokenType.STRING);
            n = node.CreateNode(tok, tok.ToString());
            node.Token.UpdateRange(tok);
            node.Nodes.Add(n);
            if (tok.Type != TokenType.STRING)
            {
                tree.Errors.Add(new ParseError("Unexpected token '" + tok.Text.Replace("\n", "") + "' found. Expected " + TokenType.STRING.ToString(), 0x1001, 0,
                                               tok.StartPos, tok.StartPos, tok.Length));
                return;
            }

            parent.Token.UpdateRange(node.Token);
        }

        private void ParseIdentifier(ParseNode parent)
        {
            Token tok;
            ParseNode n;
            ParseNode node = parent.CreateNode(scanner.GetToken(TokenType.Identifier), "Identifier");
            parent.Nodes.Add(node);

            tok = scanner.Scan(TokenType.IDENTIFIER);
            n = node.CreateNode(tok, tok.ToString());
            node.Token.UpdateRange(tok);
            node.Nodes.Add(n);
            if (tok.Type != TokenType.IDENTIFIER)
            {
                tree.Errors.Add(new ParseError("Unexpected token '" + tok.Text.Replace("\n", "") + "' found. Expected " + TokenType.IDENTIFIER.ToString(),
                                               0x1001, 0, tok.StartPos, tok.StartPos, tok.Length));
                return;
            }

            parent.Token.UpdateRange(node.Token);
        }

        private void ParseInteger(ParseNode parent)
        {
            Token tok;
            ParseNode n;
            ParseNode node = parent.CreateNode(scanner.GetToken(TokenType.Integer), "Integer");
            parent.Nodes.Add(node);

            tok = scanner.Scan(TokenType.INTEGER);
            n = node.CreateNode(tok, tok.ToString());
            node.Token.UpdateRange(tok);
            node.Nodes.Add(n);
            if (tok.Type != TokenType.INTEGER)
            {
                tree.Errors.Add(new ParseError("Unexpected token '" + tok.Text.Replace("\n", "") + "' found. Expected " + TokenType.INTEGER.ToString(), 0x1001,
                                               0, tok.StartPos, tok.StartPos, tok.Length));
                return;
            }

            parent.Token.UpdateRange(node.Token);
        }

        private void ParseValue(ParseNode parent)
        {
            Token tok;
            ParseNode n;
            ParseNode node = parent.CreateNode(scanner.GetToken(TokenType.Value), "Value");
            parent.Nodes.Add(node);

            tok = scanner.LookAhead(TokenType.STRING, TokenType.IDENTIFIER, TokenType.INTEGER);
            switch (tok.Type)
            {
                case TokenType.STRING:
                    ParseString(node);
                    break;
                case TokenType.IDENTIFIER:
                    ParseIdentifier(node);
                    break;
                case TokenType.INTEGER:
                    ParseInteger(node);
                    break;
                default:
                    tree.Errors.Add(new ParseError("Unexpected token '" + tok.Text.Replace("\n", "") + "' found.", 0x0002, 0, tok.StartPos, tok.StartPos,
                                                   tok.Length));
                    break;
            }

            parent.Token.UpdateRange(node.Token);
        }

        private void ParseParameters(ParseNode parent)
        {
            Token tok;
            ParseNode n;
            ParseNode node = parent.CreateNode(scanner.GetToken(TokenType.Parameters), "Parameters");
            parent.Nodes.Add(node);

            tok = scanner.LookAhead(TokenType.IDENTIFIER);
            while (tok.Type == TokenType.IDENTIFIER)
            {
                ParseIdentifier(node);

                tok = scanner.Scan(TokenType.EQUAL);
                n = node.CreateNode(tok, tok.ToString());
                node.Token.UpdateRange(tok);
                node.Nodes.Add(n);
                if (tok.Type != TokenType.EQUAL)
                {
                    tree.Errors.Add(new ParseError("Unexpected token '" + tok.Text.Replace("\n", "") + "' found. Expected " + TokenType.EQUAL.ToString(), 0x1001,
                                                   0, tok.StartPos, tok.StartPos, tok.Length));
                    return;
                }

                ParseValue(node);
                tok = scanner.LookAhead(TokenType.IDENTIFIER);
            }

            parent.Token.UpdateRange(node.Token);
        }

        private void ParseCommandWithParameters(ParseNode parent)
        {
            Token tok;
            ParseNode n;
            ParseNode node = parent.CreateNode(scanner.GetToken(TokenType.CommandWithParameters), "CommandWithParameters");
            parent.Nodes.Add(node);

            ParseIdentifier(node);

            ParseParameters(node);

            parent.Token.UpdateRange(node.Token);
        }

        private void ParseCqlCommand(ParseNode parent)
        {
            Token tok;
            ParseNode n;
            ParseNode node = parent.CreateNode(scanner.GetToken(TokenType.CqlCommand), "CqlCommand");
            parent.Nodes.Add(node);

            tok = scanner.Scan(TokenType.EVERYTHING_BUT_START_WITH_BANG);
            n = node.CreateNode(tok, tok.ToString());
            node.Token.UpdateRange(tok);
            node.Nodes.Add(n);
            if (tok.Type != TokenType.EVERYTHING_BUT_START_WITH_BANG)
            {
                tree.Errors.Add(
                        new ParseError(
                                "Unexpected token '" + tok.Text.Replace("\n", "") + "' found. Expected " + TokenType.EVERYTHING_BUT_START_WITH_BANG.ToString(),
                                0x1001, 0, tok.StartPos, tok.StartPos, tok.Length));
                return;
            }

            parent.Token.UpdateRange(node.Token);
        }

        private void ParseStart(ParseNode parent)
        {
            Token tok;
            ParseNode n;
            ParseNode node = parent.CreateNode(scanner.GetToken(TokenType.Start), "Start");
            parent.Nodes.Add(node);

            tok = scanner.LookAhead(TokenType.BANG, TokenType.EVERYTHING_BUT_START_WITH_BANG);
            switch (tok.Type)
            {
                case TokenType.BANG:

                    tok = scanner.Scan(TokenType.BANG);
                    n = node.CreateNode(tok, tok.ToString());
                    node.Token.UpdateRange(tok);
                    node.Nodes.Add(n);
                    if (tok.Type != TokenType.BANG)
                    {
                        tree.Errors.Add(new ParseError("Unexpected token '" + tok.Text.Replace("\n", "") + "' found. Expected " + TokenType.BANG.ToString(),
                                                       0x1001, 0, tok.StartPos, tok.StartPos, tok.Length));
                        return;
                    }

                    ParseCommandWithParameters(node);
                    break;
                case TokenType.EVERYTHING_BUT_START_WITH_BANG:
                    ParseCqlCommand(node);
                    break;
                default:
                    tree.Errors.Add(new ParseError("Unexpected token '" + tok.Text.Replace("\n", "") + "' found.", 0x0002, 0, tok.StartPos, tok.StartPos,
                                                   tok.Length));
                    break;
            }

            tok = scanner.Scan(TokenType.EOF);
            n = node.CreateNode(tok, tok.ToString());
            node.Token.UpdateRange(tok);
            node.Nodes.Add(n);
            if (tok.Type != TokenType.EOF)
            {
                tree.Errors.Add(new ParseError("Unexpected token '" + tok.Text.Replace("\n", "") + "' found. Expected " + TokenType.EOF.ToString(), 0x1001, 0,
                                               tok.StartPos, tok.StartPos, tok.Length));
                return;
            }

            parent.Token.UpdateRange(node.Token);
        }
    }
    #endregion Parser
}

#pragma warning restore 168