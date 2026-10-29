import DOMPurify from 'dompurify';

//#region BBCode Parser

import React from "react";

/* 
 * Javascript BBCode Parser
 * https://github.com/Frug/js-bbcode-parser/blob/master/bbcode-parser.js
 * @author Philip Nicolcev
 * @license MIT License
 */

type TagParams = string;

type Tag = {
    noParse?: boolean;
    createReactNode: (parser: BBCodeParser, tagParams: TagParams, tagContents: React.ReactNode) => React.ReactNode;
    validChildLookup?: {[tagName: string]: boolean};
    validParentLookup?: {[tagName: string]: boolean};
    restrictParentsTo?: string[];
    restrictChildrenTo?: string[];
};

class BBCodeParser {
    urlPattern: RegExp = /^(?:https?|file|c):(?:\/{1,3}|\\{1})[-a-zA-Z0-9:;@#%&()~_?\+=\/\\\.]*$/;
	emailPattern: RegExp = /[^\s@]+@[^\s@]+\.[^\s@]+/;
	fontFacePattern: RegExp = /^([a-z][a-z0-9_]+|"[a-z][a-z0-9_\s]+")$/i;
    private tagNames: string[] = [];
	private tagNamesNoParse: string[] = [];
	allowedColors: {[color: string]: string};
	regExpValidHexColors: RegExp = /^#?[a-fA-F0-9]{6}$/;
    private parserTags: {[tagName: string]: Tag & { 
        validChildLookup: {[tagName: string]: boolean};
        validParentLookup: {[tagName: string]: boolean};
        restrictParentsTo: string[];
        restrictChildrenTo: string[];
    }} = {};
    
    constructor(parserTags: {[tagName: string]: Tag}, parserColors: {[color: string]: string}) {
        // create tag list and lookup fields
        for (const tagName in parserTags) {
            if (!parserTags.hasOwnProperty(tagName))
                continue;
            
            if (tagName === '*') {
                this.tagNames.push('\\' + tagName);
            } else {
                this.tagNames.push(tagName);
                if ( parserTags[tagName].noParse ) {
                    this.tagNamesNoParse.push(tagName);
                }
            }

            this.parserTags[tagName] = {
                ...parserTags[tagName],
                validChildLookup: {},
                validParentLookup: {},
                restrictParentsTo: parserTags[tagName].restrictParentsTo || [],
                restrictChildrenTo: parserTags[tagName].restrictChildrenTo || []
            };

            for (let ii = this.parserTags[tagName].restrictChildrenTo.length - 1; ii >= 0; --ii) {
                this.parserTags[tagName].validChildLookup[ this.parserTags[tagName].restrictChildrenTo[ii] ] = true;
            }
            for (let ii = this.parserTags[tagName].restrictParentsTo.length - 1; ii >= 0; --ii) {
                this.parserTags[tagName].validParentLookup[ this.parserTags[tagName].restrictParentsTo[ii] ] = true;
            }
        }
        
        this.allowedColors = parserColors;
    }

    get allowedTags(): string[] {
        return this.tagNames;
    }

    /* 
	 * Create a regular expression that captures the innermost instance of a tag in an array of tags
	 * The returned RegExp captures the following in order:
	 * 1) the tag from the array that was matched
	 * 2) all (optional) parameters included in the opening tag
	 * 3) the contents surrounded by the tag
	 * 
	 * @param {type} tagsArray - the array of tags to capture
	 * @returns {RegExp}
	 */
	private createInnermostTagRegExp(tagsArray: string[]): RegExp {
		const openingTag = '\\[(' + tagsArray.join('|') + ')\\b(?:[ =]([\\w"#\\-\\:\\/= ]*?))?\\]',
			notContainingOpeningTag = '((?:(?=([^\\[]+))\\4|\\[(?!\\1\\b(?:[ =](?:[\\w"#\\-\\:\\/= ]*?))?\\]))*?)',
			closingTag = '\\[\\/\\1\\]';
			
		return new RegExp( openingTag + notContainingOpeningTag + closingTag, 'i');
	}

    /**
     * Create a regular expression that captures the innermost instance of a tag in an array of tags that is opened and not closed before the end of the string.
	 * The returned RegExp captures the following in order:
	 * 1) the tag from the array that was matched
	 * 2) all (optional) parameters included in the opening tag
	 * 3) the contents surrounded by the tag
     * 
	 * @param {type} tagsArray - the array of tags to capture
	 * @returns {RegExp}
     */
    private createInnermostOpenOnlyTagRegExp(tagsArray: string[]): RegExp {
        const openingTag = '\\[(' + tagsArray.join('|') + ')\\b(?:[ =]([\\w"#\\-\\:\\/= ]*?))?\\]',
			notContainingOpeningTag = '((?:(?=([^\\[]+))\\4|\\[(?!\\1\\b(?:[ =](?:[\\w"#\\-\\:\\/= ]*?))?\\]))*?)$';

        return new RegExp( openingTag + notContainingOpeningTag , 'i');
    }

    /**
     * Create a regular expression that captures the innermost instance of a tag in an array of tags that is opened and not closed before the end of the string.
	 * The returned RegExp captures the following in order:
	 * 1) the contents surrounded by the tag
     * 
	 * @param {type} tagName - the array of tags to capture
	 * @returns {RegExp}
     */
    private createInnermostCloseOnlyTagRegExp(tagName: string): RegExp {
        const notContainingOpeningTag = '^((?:(?=([^\\[]+))\\4|\\[(?!\\1\\b(?:[ =](?:[\\w"#\\-\\:\\/= ]*?))?\\]))*?)',
			closingTag = '\\[\\/' + tagName + '\\]';

        return new RegExp( notContainingOpeningTag + closingTag , 'i');
    }

    /*
	 * Escape the contents of a tag and mark the tag with a null unicode character.
	 * To be used in a loop with a regular expression that captures tags.
	 * Marking the tag prevents it from being matched again.
	 * 
	 * @param {type} matchStr - the full match, including the opening and closing tags
	 * @param {type} tagName - the tag that was matched
	 * @param {type} tagParams - parameters passed to the tag
	 * @param {type} tagContents - everything between the opening and closing tags
	 * @returns {String} - the full match with the tag contents escaped and the tag marked with \u0000
	 */
	private escapeInnerTags(matchStr: string, tagName: string, tagParams: TagParams, tagContents: string): string {
		tagParams = tagParams || "";
		tagContents = tagContents || "";
		tagContents = tagContents.replace(/\[/g, "&#91;").replace(/\]/g, "&#93;");
		return "[\u0000" + tagName + tagParams + "]" + tagContents + "[/\u0000" + tagName + "]";
	}

    /* 
	 * Escape all BBCodes that are inside the given tags.
	 * 
	 * @param {string} text - the text to search through
	 * @param {string[]} tags - the tags to search for
	 * @returns {string} - the full text with the required code escaped
	 */
	private escapeBBCodesInsideTags(text: string, tags: string[]): string {
		var innerMostRegExp;
		if (tags.length === 0 || text.length < 7)
			return text;
		innerMostRegExp = this.createInnermostTagRegExp(tags);
		while (
			text !== (text = text.replace(innerMostRegExp, this.escapeInnerTags))
		);
		return text.replace(/\u0000/g,'');
	}

    private processNestedTags(left: string, center: React.ReactNode, right: string, tagNames: string[]): React.ReactNode {
        const openRegExp = this.createInnermostOpenOnlyTagRegExp(tagNames);
        while (true) {
            const leftMatch = openRegExp.exec(left);
            if (leftMatch) {
                const tagName: string = leftMatch[1].toLowerCase();
                const closeRegExp = this.createInnermostCloseOnlyTagRegExp(tagName);
                const rightMatch = closeRegExp.exec(right);
                if (rightMatch) {
                    const tagParams: TagParams = leftMatch[2] || "";
                    const tagContents: React.ReactNode = <>
                        {leftMatch[3]}
                        {center}
                        {rightMatch[1]}
                    </>;
                    const tag: Tag = this.parserTags[tagName];
                    return this.processNestedTags(
                        left.substring(0, leftMatch.index),
                        tag.createReactNode(this, tagParams, tagContents),
                        right.substring(rightMatch[0].length),
                        tagNames
                    );
                }
            } else {
                return <>
                    {this.processTags(left, tagNames)}
                    {center}
                    {this.processTags(right, tagNames)}
                </>;
            }
        }
    }

    private processTags(text: string, tagNames: string[]): React.ReactNode {
		if (tagNames.length === 0 || text.length < 7)
			return text;
		
		const innerMostRegExp = this.createInnermostTagRegExp(tagNames);
		
        const match = innerMostRegExp.exec(text);
        if (match) {
            // Build the BBCode tag
            const tagName: string = match[1].toLowerCase();
            const tagParams: TagParams = match[2] || "";
            const tagContents: string = match[3] || "";
            const tag: Tag = this.parserTags[tagName];
            
            const left: string = text.substring(0, match.index);
            const center = tag.createReactNode(this, tagParams, tagContents);
            const right: string = text.substring(match.index + match[0].length);
            return this.processNestedTags(left, center, right, tagNames);
        } else {
            return text;
        }
	}

    process(text: string) {
		text = this.escapeBBCodesInsideTags(text, this.tagNamesNoParse);
		
		return this.processTags(text, this.tagNames);
	}
}

const BBCODE_PARSER: BBCodeParser = new BBCodeParser(
    {
        'b': {
            createReactNode(parser, tagParams, tagContents) {
                return <b>{tagContents}</b>;
            },
        },
        'code': {
            createReactNode(parser, tagParams, tagContents) {
                return <pre>{tagContents}</pre>;
            },
            noParse: true
        },
        'color': {
            createReactNode(parser, tagParams, tagContents) {
                var colorCode = tagParams.trim() || "inherit";
                parser.regExpValidHexColors.lastIndex = 0;
                if ( colorCode in parser.allowedColors ) {
                    return '<span style="color:' + parser.allowedColors[colorCode] + '">';
                } else {
                    if ( !parser.regExpValidHexColors.test( colorCode ) ) {
                        colorCode = "inherit";
                    } else {
                        if (colorCode.substring(0,1) !== "#") {
                            colorCode = "#" + colorCode;
                        }
                    }
                }

                return <span style={{color: colorCode}}>{tagContents}</span>;
            },
        },
        'h1': {
            createReactNode(parser, tagParams, tagContents) {
                return <h1>{tagContents}</h1>;
            },
        },
        'h2': {
            createReactNode(parser, tagParams, tagContents) {
                return <h2>{tagContents}</h2>;
            },
        },
        'h3': {
            createReactNode(parser, tagParams, tagContents) {
                return <h3>{tagContents}</h3>;
            },
        },
        'h4': {
            createReactNode(parser, tagParams, tagContents) {
                return <h4>{tagContents}</h4>;
            },
        },
        'h5': {
            createReactNode(parser, tagParams, tagContents) {
                return <h5>{tagContents}</h5>;
            },
        },
        'h6': {
            createReactNode(parser, tagParams, tagContents) {
                return <h6>{tagContents}</h6>;
            },
        },
        'i': {
            createReactNode(parser, tagParams, tagContents) {
                return <i>{tagContents}</i>;
            },
        },
        'img': {
            createReactNode(parser, tagParams, tagContents) {
                var myUrl: string = tagContents?.toString() || '';

                parser.urlPattern.lastIndex = 0;
                if ( !parser.urlPattern.test( myUrl ) ) {
                    myUrl = "";
                }

                return <img 
                    className="bbCodeImage"
                    src={myUrl}
                />;
            },
            noParse: true
        },
        'list': {
            createReactNode(parser, tagParams, tagContents) {
                return <ul>{tagContents}</ul>;
            },
            restrictChildrenTo: ["*", "li"]
        },
        'noparse': {
            createReactNode(parser, tagParams, tagContents) {
                return tagContents;
            },
            noParse: true
        },
        'quote': {
            createReactNode(parser, tagParams, tagContents) {
                return <blockquote>{tagContents}</blockquote>;
            },
        },
        's': {
            createReactNode(parser, tagParams, tagContents) {
                return <s>{tagContents}</s>;
            },
        },
        'size': {
            createReactNode(parser, tagParams, tagContents) {
                const mySizePx = parseInt(tagParams.trim(), 10) || 0;
                const mySize: string = mySizePx < 10 || mySizePx > 20 ? 'inherit' : `${mySizePx}px`;
                return <span style={{fontSize: mySize}}>{tagContents}</span>;
            },
        },
        'u': {
            createReactNode(parser, tagParams, tagContents) {
                return <span style={{textDecoration: 'underline'}}>{tagContents}</span>;
            },
        },
        'url': {
            createReactNode(parser, tagParams, tagContents) {
                let myUrl: string;

                if (!tagParams) {
                    myUrl = tagContents?.toString().replace(/<.*?>/g,"") || '';
                } else {
                    myUrl = tagParams.trim();
                }

                parser.urlPattern.lastIndex = 0;
                if ( !parser.urlPattern.test( myUrl ) ) {
                    myUrl = "#";
                }

                return <a href={myUrl}>{tagContents}</a>;
            },
        }
    },
    {
        'red': 'rgb(var(--color-error)/1)',
        'orange': 'rgb(var(--color-warning)/1)',
        'green': 'rgb(var(--color-success)/1)',
        'blue': 'rgb(var(--color-info)/1)',
        'purple': 'rgb(var(--color-primary)/1)',
        'black': 'rgb(var(--color-black)/1)',
        'white': 'rgb(var(--color-white)/1)'
    }
);

//#endregion


export type BBCodeTextCellRendererProps = {
    value: string | null 
};

export function BBCodeTextCellRenderer(props: BBCodeTextCellRendererProps): React.JSX.Element {
    const bbcodeNode = React.useMemo(() => {
        return BBCODE_PARSER.process(props.value || '');
    }, [props.value]);

    return <>{bbcodeNode}</>;
}