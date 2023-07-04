    /* keywords are case insensitive */
%option caseless
    /* we don't need yywrap() function */
%option noyywrap
    /* we don't need yyunput() function */
%option nounput
    /* we don't need input() function */
%option noinput
    /* enable location */
%option bison-bridge
%option bison-locations

%{
#include "ast.h"
#include "yacc.tab.h"
#include <errors.h>
#include <iostream>

// automatically update location
#define YY_USER_ACTION \
    yylloc->first_line = yylloc->last_line; \
    yylloc->first_column = yylloc->last_column; \
    for (int i = 0; yytext[i] != '\0'; i++) { \
        if(yytext[i] == '\n') { \
            yylloc->last_line++; \
            yylloc->last_column = 1; \
        } else { \
            yylloc->last_column++; \
        } \
    }

%}

alpha [a-zA-Z]
digit [0-9]
white_space [ \t]+
new_line "\r"|"\n"|"\r\n"
sign "+"|"-"
identifier {alpha}(_|{alpha}|{digit})*
value_int {sign}?{digit}+
value_float {sign}?{digit}+\.({digit}+)?
value_datetime '{digit}{4}-{digit}{2}-{digit}{2}{white_space}{digit}{2}:{digit}{2}:{digit}{2}'
value_string '[^']*'
single_op ";"|"("|")"|","|"*"|"="|">"|"<"|"."

%x STATE_COMMENT

%%
    /* block comment */
"/*" { BEGIN(STATE_COMMENT); }
<STATE_COMMENT>"*/" { BEGIN(INITIAL); }
<STATE_COMMENT>[^*] { /* ignore the text of the comment */ }
<STATE_COMMENT>\* { /* ignore *'s that aren't part of */ }
    /* single line comment */
"--".* { /* ignore single line comment */ }
    /* white space and new line */
{white_space} { /* ignore white space */ }
{new_line} { /* ignore new line */ }
    /* keywords */
"SHOW" { return SHOW; }
"BEGIN" { return TXN_BEGIN; }
"COMMIT" { return TXN_COMMIT; }
"ABORT" { return TXN_ABORT; }
"ROLLBACK" { return TXN_ROLLBACK; }
"TABLES" { return TABLES; }
"CREATE" { return CREATE; }
"TABLE" { return TABLE; }
"DROP" { return DROP; }
"DESC" { return DESC; }
"INSERT" { return INSERT; }
"INTO" { return INTO; }
"VALUES" { return VALUES; }
"DELETE" { return DELETE; }
"FROM" { return FROM; }
"WHERE" { return WHERE; }
"UPDATE" { return UPDATE; }
"SET" { return SET; }
"SELECT" { return SELECT; }
"INT" { return INT; }
"CHAR" { return CHAR; }
"FLOAT" { return FLOAT; }
"BIGINT" { return BIGINT; }
"DATETIME" {return DATETIME; }
"INDEX" { return INDEX; }
"AND" { return AND; }
"JOIN" {return JOIN;}
"EXIT" { return EXIT; }
"HELP" { return HELP; }
"ORDER" { return ORDER; }
"BY" {  return BY;  }
"ASC" { return ASC; }
"SUM" { return SUM; }
"MAX" { return MAX; }
"MIN" { return MIN; }
"COUNT" { return COUNT; }
"AS" { return AS; }
"LIMIT" { return LIMIT; }
    /* operators */
">=" { return GEQ; }
"<=" { return LEQ; }
"<>" { return NEQ; }
{single_op} { return yytext[0]; }
    /* id */
{identifier} {
    yylval->sv_str = yytext;
    return IDENTIFIER;
}
    /* literals */
{value_int} {
    yylval->sv_bigint = atoll(yytext);
    int n = strlen(yytext);
    std::string num = std::to_string(yylval->sv_bigint);
    // 前导零填充长度
    if(num.size() < n) {
        std::string t(n - num.size(), '0');
        if(num[0] == '+' || num[0] == '-') {
            t[0] = num[0];
            num[0] = '0';
        }
        num = t + num;
    }
    if((yytext[0] == '+' || yytext[0] == '-') && num[0] == '0') {
        num[0] = yytext[0];
    }
    if(strcmp(num.c_str(), yytext) != 0) {
        throw InvalidValueCountError();
    }
    if(yylval->sv_bigint > INT32_MAX || yylval->sv_bigint < INT32_MIN){
        return VALUE_BIGINT;
    }
    yylval->sv_int = yylval->sv_bigint;
    return VALUE_INT;
}
{value_float} {
    yylval->sv_float = atof(yytext);
    return VALUE_FLOAT;
}
{value_datetime} {
    // 判断日期合法性
    std::string datetime = std::string(yytext + 1, strlen(yytext) - 2);
    int y, m, d, h, mi, s;
    sscanf(datetime.c_str(), "%d-%d-%d %d:%d:%d", &y, &m, &d, &h, &mi, &s);
    int days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if ((y % 4 == 0 && y % 100) || y % 400 == 0) {// 闰年
        days[2] = 29;
    } else {
        days[2] = 28;
    }
    if (y < 1000 || y > 9999 || m < 1 || m > 12 || d < 1 || d > days[m] || h < 0 || h > 23 ||
        mi < 0 || mi > 59 || s < 0 || s > 59) {// 判断年份 月份 日期 时 分 秒
        yylval->sv_str = datetime;
        return VALUE_STRING;
    }
    // 合法日期
    long long res = 0;
    for(auto i : datetime){
        if(i >= '0' && i <= '9'){
            res = res * 10 + i - '0';
        }
    }
    yylval->sv_datetime = res;
    return VALUE_DATETIME;
}
{value_string} {
    yylval->sv_str = std::string(yytext + 1, strlen(yytext) - 2);
    return VALUE_STRING;
}
    /* EOF */
<<EOF>> { return T_EOF; }
    /* unexpected char */
. { std::cerr << "Lexer Error: unexpected character " << yytext[0] << std::endl; }
%%
