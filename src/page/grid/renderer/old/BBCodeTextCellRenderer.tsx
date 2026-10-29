import { ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import DOMPurify from 'dompurify';

/* 
 * Javascript BBCode Parser
 * https://github.com/Frug/js-bbcode-parser/blob/master/bbcode-parser.js
 * @author Philip Nicolcev
 * @license MIT License
 */

type TagParams = string;

type Tag = {
    noParse?: boolean;
    openTag: (parser: BBCodeParser, tagParams: TagParams, tagContents: string) => string;
    content?: (parser: BBCodeParser, tagParams: TagParams, tagContents: string) => string;
    closeTag: (parser: BBCodeParser, tagParams: TagParams, tagContents: string) => string;
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
	private createInnermostTagRegExp(tagsArray: string[]) {
		const openingTag = '\\[(' + tagsArray.join('|') + ')\\b(?:[ =]([\\w"#\\-\\:\\/= ]*?))?\\]',
			notContainingOpeningTag = '((?:(?=([^\\[]+))\\4|\\[(?!\\1\\b(?:[ =](?:[\\w"#\\-\\:\\/= ]*?))?\\]))*?)',
			closingTag = '\\[\\/\\1\\]';
			
		return new RegExp( openingTag + notContainingOpeningTag + closingTag, 'i');
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
	private escapeInnerTags(matchStr: string, tagName: string, tagParams: TagParams, tagContents: string) {
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
	private escapeBBCodesInsideTags(text: string, tags: string[]) {
		var innerMostRegExp;
		if (tags.length === 0 || text.length < 7)
			return text;
		innerMostRegExp = this.createInnermostTagRegExp(tags);
		while (
			text !== (text = text.replace(innerMostRegExp, this.escapeInnerTags))
		);
		return text.replace(/\u0000/g,'');
	}

    private processTags(text: string, tagNames: string[]) {
		var innerMostRegExp;
		
		if (tagNames.length === 0 || text.length < 7)
			return text;
		
		innerMostRegExp = this.createInnermostTagRegExp(tagNames);
		
        const replaceTagsAndContent = (matchStr: string, tagName: string, tagParams: TagParams, tagContents: string) => {
            tagName = tagName.toLowerCase();
            tagParams = tagParams || "";
            tagContents = tagContents || "";
            const tag: Tag = this.parserTags[tagName];
            return tag.openTag(this, tagParams, tagContents) + (tag.content ? tag.content(this, tagParams, tagContents) : tagContents) + tag.closeTag(this, tagParams, tagContents);
        };

		while (
			text !== (text = text.replace(innerMostRegExp, replaceTagsAndContent))
		);
		
		return text;
	}

    process(text: string) {
		text = this.escapeBBCodesInsideTags(text, this.tagNamesNoParse);
		
		return this.processTags(text, this.tagNames);
	}
}

const bbcode: BBCodeParser = new BBCodeParser(
    {
        'b': {
            openTag: function(parser,params,content) {
                return '<b>';
            },
            closeTag: function(parser,params,content) {
                return '</b>';
            }
        },
        'code': {
            openTag: function(parser,params,content) {
                return '<pre>';
            },
            closeTag: function(parser,params,content) {
                return '</pre>';
            },
            noParse: true
        },
        'color': {
            openTag: function(parser,params,content) {
                var colorCode = params.trim() || "inherit";
                console.log(colorCode);
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

                return '<span style="color:' + colorCode + '">';
            },
            closeTag: function(parser,params,content) {
                return '</span>';
            }
        },
        'h1': {
            openTag: function(parser,params,content) {
                return '<h1>';
            },
            closeTag: function(parser,params,content) {
                return '</h1>';
            }
        },
        'h2': {
            openTag: function(parser,params,content) {
                return '<h2>';
            },
            closeTag: function(parser,params,content) {
                return '</h2>';
            }
        },
        'h3': {
            openTag: function(parser,params,content) {
                return '<h3>';
            },
            closeTag: function(parser,params,content) {
                return '</h3>';
            }
        },
        'h4': {
            openTag: function(parser,params,content) {
                return '<h4>';
            },
            closeTag: function(parser,params,content) {
                return '</h4>';
            }
        },
        'h5': {
            openTag: function(parser,params,content) {
                return '<h5>';
            },
            closeTag: function(parser,params,content) {
                return '</h5>';
            }
        },
        'h6': {
            openTag: function(parser,params,content) {
                return '<h6>';
            },
            closeTag: function(parser,params,content) {
                return '</h6>';
            }
        },
        'i': {
            openTag: function(parser,params,content) {
                return '<i>';
            },
            closeTag: function(parser,params,content) {
                return '</i>';
            }
        },
        'img': {
            openTag: function(parser,params,content) {

                var myUrl = content;

                parser.urlPattern.lastIndex = 0;
                if ( !parser.urlPattern.test( myUrl ) ) {
                    myUrl = "";
                }

                return '<img class="bbCodeImage" src="' + myUrl + '">';
            },
            closeTag: function(parser,params,content) {
                return '';
            },
            content: function(parser,params,content) {
                return '';
            }
        },
        'list': {
            openTag: function(parser,params,content) {
                return '<ul>';
            },
            closeTag: function(parser,params,content) {
                return '</ul>';
            },
            restrictChildrenTo: ["*", "li"]
        },
        'noparse': {
            openTag: function(parser,params,content) {
                return '';
            },
            closeTag: function(parser,params,content) {
                return '';
            },
            noParse: true
        },
        'quote': {
            openTag: function(parser,params,content) {
                return '<blockquote>';
            },
            closeTag: function(parser,params,content) {
                return '</blockquote>';
            }
        },
        's': {
            openTag: function(parser,params,content) {
                return '<s>';
            },
            closeTag: function(parser,params,content) {
                return '</s>';
            }
        },
        'size': {
            openTag: function(parser,params,content) {
                const mySizePx = parseInt(params.trim(), 10) || 0;
                const mySize: string = mySizePx < 10 || mySizePx > 20 ? 'inherit' : `${mySizePx}px`;
                return '<span style="font-size:' + mySize + '">';
            },
            closeTag: function(parser,params,content) {
                return '</span>';
            }
        },
        'u': {
            openTag: function(parser,params,content) {
                return '<span style="text-decoration:underline">';
            },
            closeTag: function(parser,params,content) {
                return '</span>';
            }
        },
        'url': {
            openTag: function(parser,params,content) {

                var myUrl;

                if (!params) {
                    myUrl = content.replace(/<.*?>/g,"");
                } else {
                    myUrl = params.trim();
                }

                parser.urlPattern.lastIndex = 0;
                if ( !parser.urlPattern.test( myUrl ) ) {
                    myUrl = "#";
                }

                return '<a href="' + myUrl + '">';
            },
            closeTag: function(parser,params,content) {
                return '</a>';
            }
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



let nextBBCodeId: bigint = 1n;

export class BBCodeTextCellRenderer implements ICellRendererComp {
    private gui: HTMLDivElement;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.id = `bbcode-render-${nextBBCodeId++}`;
    }

    init(params: ICellRendererParams) {
        this.refresh(params);
    }

    getGui(): HTMLElement {
        return this.gui;
    }

    refresh(params: ICellRendererParams<any, any, any>): boolean {
        navigator.locks.request(this.gui.id, () => {
            // Clear prior children
            for (const child of this.gui.childNodes) {
                this.gui.removeChild(child);
            }

            // Parse Markdown formatting
            try {
                const raw: string = String(params.getValue ? (params.getValue() ?? '') : '');
                const bbcodeHtml = DOMPurify.sanitize(bbcode.process(raw));
                this.gui.innerHTML = bbcodeHtml;
            } catch (e) {
                console.error(e);
            }
        });
        return true;
    }
}