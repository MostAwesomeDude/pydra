/*
 * jQuery ActsAsTreeTable plugin 1.1
 * =================================
 *
 * License
 * -------
 *
 * Copyright (c) 2008 Ludo van den Boom
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
(function($) {
	
	// The options for this plugin should be available to all functions in this
	// plugin.
	var options;
	
	// TableTree Plugin: display a tree in a table.
	//
	// TODO Look into possibility to add alternating row colors
	$.fn.acts_as_tree_table = function(opts) {
		options = $.extend({}, $.fn.acts_as_tree_table.defaults, opts);
		
		return this.each(function() {
			var table = $(this);
			// Add class to enable styles specific to this plugin.
			table.addClass("acts_as_tree_table");
			
			// Walk through each 'node' that is part of the tree, enabling tree
			// behavior on parent/branch nodes.
			table.children("tbody").children("tbody tr").each(function() {
				var node = $(this);
				
				// Every node in the tree that has child nodes is marked as a 'parent',
				// unless this has already been done manually.
				if(node.not(".parent") && children_of(node).size() > 0) {
					node.addClass("parent");
				}
				
				// Make each .parent row collapsable by adding some html to the column
				// that is displayed as tree.
				if(node.is(".parent")) {
					init_parent(node);
				}
			});
		});
	};
	
	// Default options
	$.fn.acts_as_tree_table.defaults = {
		expandable: true,
		default_state: 'expanded',
		indent: 19,
		tree_column: 0
	};
	
	// === Private Methods
	
	// Select all children of a node.	
	function children_of(node) {
		return $("tr.child-of-" + node[0].id);
	};
	
	// Hide all descendants of a node.
	function collapse(node) {
		children_of(node).each(function() {
			var child = $(this);

			// Recursively collapse any descending nodes too
			collapse(child);

			child.hide();
		});
	};
	
	// Show all children of a node.
	function expand(node) {
		children_of(node).each(function() {
			var child = $(this);
			
			// Recursively expand any descending nodes that are parents which where
			// expanded before too.
			if(child.is(".expanded.parent")) {
				expand(child);
			}
			
			child.show();
		});
	};

	// Add stuff to cell that contains stuff to make the tree collapsable.
	function init_parent(node) {
		// Select cell in column that should display the tree
		var cell = $(node.children("td")[options.tree_column]);

		// Calculate left padding
		var padding = parseInt(cell.css("padding-left")) + options.indent;
		
		children_of(node).each(function() {
			$($(this).children("td")[options.tree_column]).css("padding-left", padding + "px");
		});

		if(options.expandable) {
			// Add clickable expander buttons (plus and minus icon thingies)
			//
			// Why do I use a z-index on the expander?
			// Otherwise the expander would not be visible in Safari/Webkit browsers.
			cell.prepend('<span style="margin-left: -' + options.indent + 'px; padding-left: ' + options.indent + 'px" class="expander"></span>');
			var expander = $(cell[0].firstChild);
			expander.click(function() { toggle(node); });
			
			// check for a class set explicitly by the user, otherwise set the default class
			if (! (node.hasClass("expanded") || node.hasClass("collapsed"))) {
			    node.addClass(options.default_state);
			}
			
			// apply the default state
			if(node.hasClass("collapsed")) {
                            collapse(node);
                        } else if (node.hasClass("expanded")) {
                            expand(node);
                        }
		}
	};
	
	// Toggle a node
	function toggle(node) {
		if(node.is(".collapsed")) {
			node.removeClass("collapsed");
			node.addClass("expanded");
			expand(node);
		}
		else {
			node.removeClass("expanded");
			node.addClass("collapsed");
			collapse(node);
		}
	};
	
	// extend function to jquery
	$.fn.collapse = function(){
	   collapse(this);
	};

        // extend function to jquery
        $.fn.expand = function(){
	   expand(this);
	};
	
	// extend function to jquery
	$.fn.toggle = function(){
	   toggle(this);
	};
	
})(jQuery);