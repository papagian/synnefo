var mydata; // temp

$(document).ready(function() {

		var $actionbar = $('.actionbar');

	if($actionbar.length > 0) {
        sticker();
	}
	else {
		$('.compact-view').addClass('no-margin-left');
	}

	var $lastClicked = null;
	var $prevClicked = null;
	var selected = {
		items: [],
		actions: {}
	};

	var availableActions = {};
	var allowedActions= {};

	/* Actionbar */
	$('.actionbar a').each(function() {
		availableActions[$(this).data('action')] = true;
	});

	for(var prop in availableActions) {
		allowedActions[prop] = true;
	}

	/* If the sidebar link is not disabled show the corresponding modal */
	$('.actionbar a').click(function(e) {
		if($(this).hasClass('disabled')) {
			e.preventDefault();
			e.stopPropagation();
		}
		else {
			var modal = $(this).data('target');
			drawModal(modal);
		}
	});


	/* Table */
	/* For the tables we have used DataTables 1.10.0 */
	var url = $('#table-items-total').data("url");
	var serverside = Boolean($('#table-items-total').data("server-side"));
	var table;
	// var tableSelected;
	$.fn.dataTable.ext.legacy.ajax = true;
	var extraData;
	// sets the classes of the btns that are used for navigation throw the pages (next, prev, 1, 2, 3...)
	// $.fn.dataTableExt.oStdClasses.sPageButton = "btn btn-primary";
	var maxCellChar = 18;
	var tableDomID = '#table-items-total';
	var tableSelectedDomID = '#table-items-selected'
	var tableMassiveDomID = '#total-list'
	table = $(tableDomID).DataTable({
		"autoWidth": false,
		"paging": true,
		"searching": false,
		// "stateSave": true,
		"processing": true,
		"serverSide": serverside,
		"ajax": {
			"url": url,
			"data": function(data, callback, settings) {

				var prefix = 'sSearch_';

				if(!_.isEmpty(filters)) {
					for (var prop in filters) {
						data[prefix+prop] = filters[prop];
					}
				}
			},
			"dataSrc" : function(response) {
				mydata = response;
				extraData = response.extra;
				if(response.aaData.length != 0) {
					var rowsArray = response.aaData;
					var rowL = rowsArray.length;
					var extraCol = rowsArray[0].length; //last column
					for (var i=0; i<rowL; i++) {
						rowsArray[i][extraCol] = response.extra[i]
					}
				}
				return response.aaData;
			}
		},
		"columnDefs": [
			{
				"targets": 0,
				"render": function(data, type, rowData) {
					return checkboxTemplate(data, 'unchecked');
				}
			},
			{
				"targets": -1, // the first column counting from the right is "Summary"
				"orderable": false,
				"render": function(data, type, rowData) {
					return extraTemplate(data);
				}
			},
			// "targets": '_all' this must be the last item of the array
			{
				"targets": '_all',
				"render": function( data, type, row, meta ) {
					if(data.length > maxCellChar) {
						return _.template(snf.tables.html.trimedCell, {data: data, trimmedData: data.substring(0, maxCellChar)});
					}
					else {
						return data;
					}
				}
			},
		],
		"order": [0, "asc"],
		"createdRow": function(row, data, dataIndex) {
			var extraIndex = data.length - 1;
			row.id = data[extraIndex].id.value; //sets the dom id
			clickSummary(row);
			clickDetails(row);
		},

		"dom": '<"custom-buttons">frtilp',
		"language" : {
			"sLengthMenu": 'Pagination _MENU_'
		},
		"drawCallback": function(settings) {
			isSelected();
			updateToggleAllSelect();
			$("[data-toggle=popover]").popover();
		}
	});

	if($actionbar.length > 0) {
		var btns = snf.tables.html.reloadTable + snf.tables.html.selectPageBtn + snf.tables.html.selectAllBtn + snf.tables.html.clearSelected + snf.tables.html.toggleSelected
		$("div.custom-buttons:not(.bottom)").html(btns);
	}
	else {
		$("div.custom-buttons:not(.bottom)").html(snf.tables.html.reloadTable);
	}

	$('.container').on('click', '.reload-table', function(e) {
		e.preventDefault();
		$(tableDomID).dataTable().api().ajax.reload();
	});
	$('.notify').on('click', '.clear-reload', function(e) {
		e.preventDefault();
		resetAll(tableDomID);
		$(tableDomID).dataTable().api().ajax.reload();

	})


	function isSelected() {
		var tableLength = table.rows()[0].length;
		var selectedL = selected.items.length;
		if(selectedL !== 0 && tableLength !== 0) {
			var dataLength = table.row(0).data().length
			var extraIndex = dataLength - 1;
			for(var j = 0; j<tableLength; j++) { // index of rows start from zero
				for(var i = 0; i<selectedL; i++){
					if (selected.items[i].id === table.row(j).data()[extraIndex].id.value) {
						$(table.row(j).nodes()).addClass('selected');
						$(table.row(j).nodes()).find('td:first-child .selection-indicator').addClass('snf-checkbox-checked').removeClass('snf-checkbox-unchecked');
						break;
					}
				}
			}
		}
	}

	var newTable = true;
	$('.select-all-confirm').click(function(e) {
		$(this).closest('.modal').addClass('in-progress');
		if(newTable) {
			newTable = false;
			countme = true;
			$(tableMassiveDomID).DataTable({
				"paging": false,
				"processing": false,
				"serverSide": true,
				"ajax": {
					"url": url,
					"data": function(data, callback, settings) {

						var prefix = 'sSearch_';

						if(!$.isEmptyObject(filters)) {
							for (var prop in filters) {
								data[prefix+prop] = filters[prop];
							}
						}
					},

					"dataSrc" : function(response) {
						alldata = response;
						extraData = response.extra;
						if(response.aaData.length != 0) {
							var rowsArray = response.aaData;
							var rowL = rowsArray.length;
							var extraCol = rowsArray[0].length; //last column
							for (var i=0; i<rowL; i++) {
								rowsArray[i][extraCol] = response.extra[i];
							}
						}
						return response.aaData;
					}
				},
				createdRow: function(row, data, dataIndex) {
					if(countme) {
						countme = false;
					}
					var info = data[data.length - 1];
					var newItem = addItem(info);
					if(newItem !== null) {
						enableActions(newItem.actions);
						keepSelected(data);
							if(dataIndex>=500 && dataIndex%500 === 0) {
									setTimeout(function() {
										return true;
									}, 50);
							}
					}
				},
				"drawCallback": function(settings) {
					isSelected();
					updateCounter('.selected-num')
					$('#massive-actions-warning').modal('hide')
					$('#massive-actions-warning').removeClass('in-progress')
					tableSelected.rows().draw();
					updateToggleAllSelect();
					updateClearAll();
				}
			});
		}
		else {
			$(tableMassiveDomID).dataTable().api().ajax.reload();
		}
	});

	tableSelected = $(tableSelectedDomID).DataTable({
		// "stateSave": true,
		"columnDefs": [
		{
			"targets": 0,
			"render": function(data, type, rowData) {
				return checkboxTemplate(data, 'checked');
			}
		},
		{
			"targets": -1, // the first column counting from the right is "Summary"
			"orderable": false,
			"render": function(data, type, rowData) {
				return extraTemplate(data);
			},
		},
			// "targets": '_all' this must be the last item of the array
			{
				"targets": '_all',
				"render": function( data, type, row, meta ) {
					if(data.length > maxCellChar) {
						return _.template(snf.tables.html.trimedCell, {data: data, trimmedData: data.substring(0, maxCellChar)});
					}
					else {
						return data;
					}
				}
			},
		],
		"order": [0, "asc"],
		"lengthMenu": [[5, 10, 25, 50, -1], [5, 10, 25, 50, "All"]],
		"dom": 'frtilp',
		"language" : {
			"sLengthMenu": 'Pagination _MENU_'
		},
		"createdRow": function(row, data, dataIndex) {
			var extraIndex = data.length - 1;
			row.id = 'selected-'+data[extraIndex].id.value; //sets the dom id
			clickDetails(row);
			clickSummary(row);
		},
	});

	function keepSelected(data, drawNow) {
		//return;
		if(drawNow) {
			tableSelected.row.add(data).draw();
		}
		else
			tableSelected.row.add(data).node();
	};


	/* Removes a row from the table of selected items */
	function removeSelected(rowID) {
		if(rowID === true) {
			tableSelected.clear().draw()
		}
		else {
			tableSelected.row('#selected-'+rowID).remove().draw();
		}
	};

	/* Applies style that indicates that a row from the main table is not selected */
	function deselectRow(itemID) {
		table.row('#'+itemID).nodes().to$().removeClass('selected');
		$(table.row('#'+itemID).nodes()).find('td:first-child .selection-indicator').addClass('snf-checkbox-unchecked').removeClass('snf-checkbox-checked');
	}

	function updateDisplaySelected() {
		if(selected.items.length > 0) {
			$('a.toggle-selected').removeClass('disabled');
		}
		else {
			$('a.toggle-selected').addClass('disabled');
		}
	}

	$(tableSelectedDomID).on('click', 'tbody tr td:first-child .selection-indicator', function() {
		var $tr = $(this).closest('tr');
		var column = $tr.find('td').length - 1;
		var $trID = $tr.attr('id');
		var selectedRow = tableSelected.row('#'+$trID);
		var itemID = tableSelected.cell('#'+$trID, column).data().id.value;
		$tr.find('td:first-child .selection-indicator').addClass('snf-checkbox-unchecked').removeClass('snf-checkbox-checked');
		$tr.fadeOut('slow', function() {
			selectedRow.remove().draw();
			table.row('#'+itemID).nodes().to$().removeClass('selected');
			$(table.row('#'+itemID).nodes()).find('td:first-child .selection-indicator').addClass('snf-checkbox-unchecked').removeClass('snf-checkbox-checked');
			deselectRow(itemID)

		});
		removeItem(itemID);
		enableActions(undefined, true);
		updateCounter('.selected-num');
		updateToggleAllSelect();
	});


	$(tableDomID).on('click', 'tbody tr .selection-indicator', function(e) {
		$prevClicked = $lastClicked;
		$lastClicked =  $(this).closest('tr');
		if(!e.shiftKey) {
			selectRow($lastClicked, e.type);
		}
		else {
			var select;
			if($lastClicked.hasClass('selected')) {
				select = false;
			}
			else {
				select = true;
			}
			if(e.shiftKey && $prevClicked !== null && $lastClicked !== null) {
				var startRow;
				var start = $prevClicked.index();
				var end = $lastClicked.index();
				if(start < end) {
					startRow = $prevClicked;
					for (var i = start; i<=end; i++) {
						if((select && !($(startRow).hasClass('selected'))) || (!select && $(startRow).hasClass('selected'))) {
							selectRow(startRow);
						}
						startRow = startRow.next();
					}
				}
				else if(end < start) {
					startRow = $prevClicked;
					for (var i = start; i>=end; i--) {
						if((select && !($(startRow).hasClass('selected'))) || (!select && $(startRow).hasClass('selected'))) {
							selectRow(startRow);
						}
						startRow = startRow.prev();
					}
				}
			}
		}
		updateClearAll();
	});

	$(document).bind('keydown', function(e){
		if(e.shiftKey && !$(e.target).is('input') && !$(e.target).is('textarea')) {
			$(tableDomID).addClass('with-shift')
		}
	});

	$(document).bind('keyup', function(e){
		if(e.which === 16 && !$(e.target).is('input') && !$(e.target).is('textarea')) {
			deselectText();
			$(tableDomID).removeClass('with-shift')
		}
	});

	function deselectText() {
	if (window.getSelection) {
		if (window.getSelection().empty) {  // Chrome
			window.getSelection().empty();
		} else if (window.getSelection().removeAllRanges) {  // Firefox
			window.getSelection().removeAllRanges();
		}
		} else if (document.selection) {  // IE?
			document.selection.empty();
		}
	}

	function selectRow(row) {
		var $row = $(row);
		$row.find('td:first-child .selection-indicator').toggleClass('snf-checkbox-checked snf-checkbox-unchecked');
		var infoRow = table.row($row).data();
		var info = infoRow[infoRow.length - 1]
		// var info = $(tableDomID).dataTable().api().cell($row.find('td:last-child')).data();
		if($row.hasClass('selected')) {
			$row.removeClass('selected');
			removeItem(info.id.value);
			enableActions(undefined, true);
			removeSelected($row.attr('id'));
		}
		else {
			$row.addClass('selected');
			var newItem = addItem(info);
			enableActions(newItem.actions)
			selData = table.row($row).data();

			keepSelected(selData, true);
		}
		updateCounter('.selected-num');
		updateToggleAllSelect();
	};

	function updateCounter(counterDOM, num) {
		var $counter = $(counterDOM);
		if(num) {
			$counter.text(num);			
		}
		else {
			$counter.text(selected.items.length);
		}
	};

	function checkboxTemplate(data, initState) {
		if(data.length > maxCellChar) {
			data = _.template(snf.tables.html.trimedCell, {data: data, trimmedData: data.substring(0, maxCellChar)});
		}
		if($actionbar.length > 0)
			return _.template(snf.tables.html.checkboxCell, {state: initState, content: data});
		else
			return data;
	}

	function extraTemplate(data) {
			var list = '';
			var html;
			var hasDetails = false;
			for(var prop in data) {
				if(prop !== "details_url") {
					if(data[prop].visible) {
						list += _.template(snf.tables.html.summaryLine, {key: data[prop].display_name, value: data[prop].value});
					}
				}
				else {
					hasDetails = true;
				}
			}
		if(hasDetails) {
			html = _.template(snf.tables.html.detailsBtn, {url: data["details_url"].value}) + _.template(snf.tables.html.summary, {list: list});
		}
		else {
			html = _.template(snf.tables.html.summary, {list: list});
		}
			return html;
	};

	function clickDetails(row) {
		$(row).find('td:last-child a.details-link').click(function(e) {
			e.stopPropagation();
		})
	}

	function clickSummary(row) {
		$(row).find('td:last-child a.expand-area').click(function(e) {
			e.preventDefault();
        
			var $summaryTd = $(this).closest('td');
			var $btn = $summaryTd.find('.expand-area');
			var $btnIcon = $btn.find('span');
			var $summaryContent = $summaryTd.find('.info-summary');
			
			var summaryContentWidth = $summaryTd.closest('tr').width();
			var summaryContentHeight = $summaryTd.closest('tr').height() - parseInt($summaryTd.css('padding-top')) - $btn.height()- parseInt($summaryTd.css('padding-bottom')) ;
			var summaryContPos = summaryContentWidth - $summaryTd.width()+ parseInt($summaryTd.css('padding-left'));

            if ( $btnIcon.hasClass('snf-angle-down')) {
                $summaryContent.css({
                    width: summaryContentWidth,
                    right: summaryContPos,
                    paddingTop: summaryContentHeight,
                });
            }
		    
            $btnIcon.toggleClass('snf-angle-up snf-angle-down');
			$summaryContent.stop().slideToggle(600, function() {
				if ($summaryContent.is(':visible')) {
					$btnIcon.removeClass('snf-angle-down').addClass('snf-angle-up');    
				}
				else {
					$btnIcon.removeClass('snf-angle-up').addClass('snf-angle-down');
				}
			});
		})
	};


	function addItem(infoObj) {
		var $selectedNum = $('.actionbar a').find('.selected-num');
		var itemsL;
		var newItem = {}
		var isNew = true;
		var actionsArray = infoObj.allowed_actions.value;
		var actionsL = actionsArray.length;
		var newItem = {
		   "id": infoObj.id.value,
		   "item_name": infoObj.item_name.value,
		   "contact_id": infoObj.contact_id.value,
		   "contact_name": infoObj.contact_name.value,
		   "contact_email": infoObj.contact_email.value,
		   "actions": {}
		}

		itemsL = selected.items.length;
			for(var i=0; i<itemsL; i++) {
				if(selected.items[i].id === newItem.id) {
					isNew = false;
					break;
				}
			}
		if(isNew) {
			for (var i = 0; i<actionsL; i++) {
				newItem.actions[actionsArray[i]] = true;
			}
			for(var prop in availableActions) {
				if(!(prop in newItem.actions)) {
					newItem.actions[prop] = false;
				}
			}
			selected.items.push(newItem);
			return newItem
		}
		else
			return null;
	};

	function removeItem(itemID) {
		var items = selected.items;
		var itemsL = items.length;
		for (var i = 0; i<itemsL; i++) {
			if(String(items[i].id) === String(itemID)) {
				selected.items.splice(i, 1);
				break;
			}
		}
	};


	/* It enables the btn (link) of the corresponding allowed action */
	function enableActions(actionsObj, removeItemFlag) {
		var itemActionsL =selected.items.length;
		var $actionBar = $('.actionbar');
		var itemActions = {};
		if (removeItemFlag) {
			if(!selected.items.length) {
				for(var prop in allowedActions) {
					allowedActions[prop] = false;
				}
			}
			else {
				for(var prop in allowedActions) {
					allowedActions[prop] =true;
					for(var i=0; i<itemActionsL; i++) {
						allowedActions[prop] = allowedActions[prop] && selected.items[i].actions[prop];
					}
				}
			}
		}
		else {
			if(selected.items.length === 1) {
				for(var prop in allowedActions) {
					allowedActions[prop] = availableActions[prop] && actionsObj[prop];
				}
			}
			else {
				for(var prop in allowedActions) {
					allowedActions[prop] = allowedActions[prop] && actionsObj[prop];
				}
			}
		}
		for(var prop in allowedActions) {
			if(allowedActions[prop]) {
				$actionBar.find('a[data-action='+prop+']').removeClass('disabled');
			}
			else {
				$actionBar.find('a[data-action='+prop+']').addClass('disabled');
			}
		}
	};

	function resetAll(tableDomID) {
		selected.items = [];
		removeSelected(true); //removes all selected items from the table of selected items
		updateCounter('.selected-num');
		enableActions(undefined, true);
		$(table.rows('.selected').nodes()).find('td:first-child .selection-indicator').toggleClass('snf-checkbox-checked snf-checkbox-unchecked');
		$(tableDomID).dataTable().api().rows('.selected').nodes().to$().removeClass('selected');

		updateToggleAllSelect();
		updateClearAll();
	};


	 /* select-page button */

	$('#select-page').click(function(e) {
		e.preventDefault();
		toggleVisSelected(tableDomID, $(this).hasClass('select'));
		updateClearAll();
	});


	/* select-page / deselect-page */
	function toggleVisSelected(tableDomID, selectFlag) {
		$lastClicked = null;
		$prevClicked = null;
		if(selectFlag) {
			$(tableDomID).find('tbody tr:not(.selected)').each(function() { // temp : shouldn't have a func that calls a named func
				selectRow(this);
			});
		}
		else {
			$(tableDomID).find('tbody tr.selected').each(function() { // temp : shouldn't have a func that calls a named func
				selectRow(this);
			});
		}
	};

	/* Checks how many rows are selected and adjusts the classes and
	the text of the select-qll btn */
	function updateToggleAllSelect() {
		var $togglePageItems = $('#select-page');
		var $label = $togglePageItems.find('span')
		var $tr = $(tableDomID).find('tbody tr');
		if($tr.length >= 1) {
			var allSelected = true
			$tr.each(function() {
				allSelected = allSelected && $(this).hasClass('selected');
				return allSelected;
			});
			if($togglePageItems.hasClass('select') && allSelected) {
				$togglePageItems.addClass('deselect').removeClass('select');
				$label.text('Deselect Page')
			}
			else if($togglePageItems.hasClass('deselect') && !allSelected) {
				$togglePageItems.addClass('select').removeClass('deselect');
				$label.text('Select Page')
			}
		}
		else {
			$togglePageItems.addClass('select').removeClass('deselect')
			$label.text('Select Page')
		}
	};

	function updateClearAll() {
		var $clearAllBtn = $('#clear-all')
		if(selected.items.length === 0) {
			$clearAllBtn.addClass('disabled');
		}
		else {
			$clearAllBtn.removeClass('disabled');
		}
	}


		/* Modals */

	function removeWarningDupl(modal) {
		var $modal = $(modal);
		$modal.find('.warning-duplicate').remove();
	}

	function resetToggleAllBtn(modal) {
		var $modal = $(modal);
		$modal.find('.toggle-more').removeClass('open').addClass('closed');
		$modal.find('.toggle-more').find('span').text('Show all');
	}
	$('.modal .cancel').click(function(e) {
		$('[data-toggle="popover"]').popover('hide');
		var $modal = $(this).closest('.modal');
		snf.modals.resetErrors($modal);
		snf.modals.resetInputs($modal);
		removeWarningDupl($modal);
		resetToggleAllBtn($modal);
		// resetAll(tableDomID);
		updateToggleAllSelect();
		updateClearAll();
		enableActions(undefined, true);
	});

	$('.modal .clear-all-confirm').click(function() {
		resetAll(tableDomID);
	});
	var $notificationArea = $('.notify');
	var countAction = 0;
	$('.modal .apply-action').click(function(e) {
		var $modal = $(this).closest('.modal');
		var noError = true;
		var itemsNum = $modal.find('tbody tr').length;
		if(selected.items.length === 0) {
			snf.modals.showError($modal, 'no-selected');
			noError = false;
		}
		if($modal.attr('data-type') === 'contact') {
			var validForm = snf.modals.validateContactForm($modal);
			noError = noError && validForm;
		}
		if(!noError) {
			e.preventDefault();
			e.stopPropagation();
		}
		else {
			$('[data-toggle="popover"]').popover('hide');
			snf.modals.performAction($modal, $notificationArea, snf.modals.html.notifyReloadTable, itemsNum, countAction);
			snf.modals.resetErrors($modal);
			snf.modals.resetInputs($modal);
			removeWarningDupl($modal);
			resetAll(tableDomID);
			resetToggleAllBtn($modal);
			countAction++;
		}
	});

	/* remove an item after the modal is visible */
	$('.modal').on('click', '.remove', function(e) {
		e.preventDefault();
		var $modal = $(this).closest('.modal')
		var $actionBtn = $modal.find('.modal-footer .apply-action');
		var $num = $modal.find('.num');
		var $tr = $(this).closest('tr');
		var itemID = $tr.attr('data-itemid');
		var idsArray = [];
		deselectRow(itemID);
		removeSelected(itemID);
		removeItem(itemID);
		idsArray = $actionBtn.attr('data-ids').replace('[', '').replace(']', '').split(',');
		var index = idsArray.indexOf(itemID);
		idsArray.splice(index, 1);

		$actionBtn.attr('data-ids','[' + idsArray + ']');
		$tr.slideUp('slow', function() {
			$(this).siblings('.hidden-row').first().css('display', 'table-row');
			$(this).siblings('.hidden-row').first().removeClass('hidden-row');
			if($(this).siblings('.hidden-row').length === 0) {
				$modal.find('.toggle-more').hide();
			}
				$(this).remove();
		});
		$num.html(idsArray.length); // should this use updateCounter?
		updateCounter('.selected-num');
	});


	function drawModal(modalID) {
		var $tableBody = $(modalID).find('.table-selected tbody');
		var modalType = $(modalID).attr('data-type');
		var itemType = $(modalID).attr('data-item');
		var $counter = $(modalID).find('.num');
		var rowsNum = selected.items.length;
		var $actionBtn = $(modalID).find('.apply-action');
		var maxVisible = 5;
		var currentRow;
		var htmlRows = '';
		var unique = true;
		var uniqueProp = '';
		var count = 0;
		var idsArray = [];
		var warningMsg = snf.modals.html.warningDuplicates;
		var warningInserted = false;
		var associations = {};
		var $btn = $(modalID).find('.toggle-more');
		$tableBody.empty();
		if(modalType === "contact") {
			uniqueProp = 'contact_id';
			for(var i=0; i<rowsNum; i++) {
				var currContactID = selected.items[i][uniqueProp];
				if(associations[currContactID] === undefined) {
					associations[currContactID] = [selected.items[i]['item_name']];
				}
				else {
					selected.items[i]['notFirst'] = true; // not the first item with the current contact_id
					associations[currContactID].push(selected.items[i]['item_name']);
				}
				if(!warningInserted && selected.items[i]['notFirst']) {
					$tableBody.closest('table').before(warningMsg);
					warningInserted = true;
				}
			}
			for(var i=0; i<rowsNum; i++) {
				if (!selected.items[i]['notFirst']) {
					idsArray.push(selected.items[i][uniqueProp]);
					currentRow = _.template(snf.modals.html.contactRow, {itemID: selected.items[i].contact_id, showAssociations: (itemType !== 'user'), associations: associations[selected.items[i][uniqueProp]].toString().replace(/\,/gi, ', '), fullName: selected.items[i].contact_name, email: selected.items[i].contact_email, hidden: (i >maxVisible)})
					htmlRows += currentRow;
				}
			}
		}

		else {
			uniqueProp = 'id';
			for(var i=0; i<rowsNum; i++) {
				idsArray.push(selected.items[i][uniqueProp]);
				currentRow = _.template(snf.modals.html.commonRow, {itemID: selected.items[i].id, itemName: selected.items[i].item_name, ownerEmail: selected.items[i].contact_email, ownerName: selected.items[i].contact_name, hidden: (i >=maxVisible)})
				htmlRows += currentRow;
			}
		}
		$tableBody.append(htmlRows); // should change
		$actionBtn.attr('data-ids','['+idsArray+']');
		updateCounter($counter, idsArray.length);

		if(idsArray.length >= maxVisible) {
			$btn.css('display', 'block');
		}
		else {
			$btn.css('display', 'none');
		}
		delete associations;
	};

	$('.modal .toggle-more').click( function() {
		var $tableBody = $(this).closest('.modal').find('table');
		if($(this).hasClass('closed')) {
			$(this).find('span').text('Show less');
			$tableBody.find('.hidden-row').slideDown('slow');
		}
		else {
			var that = this;
			$tableBody.find('tr.hidden-row').slideUp('slow', function() {
				$(that).find('span').text('Show all');
			});
		}
		$(this).toggleClass('closed open');
		});




	$('.toggle-selected').click(function (e) {
		e.preventDefault();
		var $label = $(this).find('.text');
		var label1 = 'Show selected'
		var label2 = 'Hide selected'
		$(this).toggleClass('open');
		if($(this).hasClass('open')) {
			$('#table-items-selected_wrapper').slideDown('slow', function() {
				$label.text(label2);
			});
		}
		else {
			$('#table-items-selected_wrapper').slideUp('slow', function() {
				$label.text(label1);
			});
		}
	});

	 /* Filters */

	var filters = {};
	$('.filter:not(.hidden)').first().find('input').focus();

	//var filtersHeightTotal = $('.filters').css('height');
	$('.filters').css('min-height', '60px');

	/* Standard View Functionality */

	function dropdownSelect(filterEl) {
		var $dropdownList = $(filterEl).find('.choices');

		$dropdownList.find('li a').click(function(e) {
			e.preventDefault();
			var $li = $(this).closest('li');
			var key = $(this).closest(filterEl).data('filter');
			var value = $(this).text();
			if($(this).closest('.filter-dropdown').hasClass('filter-boolean')) {
				if($li.hasClass('reset')) {
					delete filters[key];
					$li.find('.selection-indicator').removeClass('snf-radio-unchecked').addClass('snf-radio-checked');
					$li.addClass('active');
					$li.siblings('.active').find('.selection-indicator').removeClass('snf-radio-checked').addClass('snf-radio-unchecked');
					$li.siblings('.active').removeClass('active');
					$(this).closest(filterEl).find('.selected-value').text(String(value));
				}
				else {
					$li.toggleClass('active')
					if($li.hasClass('active')) {
						$li.find('.selection-indicator').removeClass('snf-radio-unchecked').addClass('snf-radio-checked');
						$li.siblings('li').removeClass('active');
						$li.siblings('li').find('.selection-indicator').removeClass('snf-radio-checked').addClass('snf-radio-unchecked');
						$(this).closest(filterEl).find('.selected-value').text(value);
						filters[key] = value;
					}
					else {
						delete filters[key];
						var resetLabel = $li.siblings('.reset').text();
						$li.siblings('li.reset').addClass('active');
						$li.siblings('li.reset').find('.selection-indicator').removeClass('snf-radio-unchecked').addClass('snf-radio-checked');
						$(this).closest(filterEl).find('.selected-value').text(resetLabel);
					}
				}
			}
			// multichoice filter
			else {
				if($li.hasClass('reset')) {
					delete filters[key];
					$li.find('.selection-indicator').toggleClass('snf-checkbox-unchecked snf-checkbox-checked');
					$li.addClass('active');

					$li.siblings('.active').find('.selection-indicator').toggleClass('snf-checkbox-unchecked snf-checkbox-checked');
					$li.siblings('.active').removeClass('active');
					$(this).closest(filterEl).find('.selected-value').text(value);
				}
				else {
					$li.toggleClass('active');
					$li.find('.selection-indicator').toggleClass('snf-checkbox-unchecked snf-checkbox-checked');
					if($li.hasClass('active')) {
						$li.siblings('.reset').removeClass('active')
						$li.siblings('.reset').find('.selection-indicator').removeClass('snf-checkbox-checked').addClass('snf-checkbox-unchecked');
						if($li.siblings('.active').length > 0) {
							arrayFilter(filters, key, value);
							$(this).closest(filterEl).find('.selected-value').append(', '+value)
						}
						else {
							$(this).closest(filterEl).find('.selected-value').text(value);
							filters[key] = [value]
						}
					}
					// deselect a choice
					else {
						if($li.siblings('.active').length >0) {
							arrayFilter(filters, key, value, true);
							$(this).closest(filterEl).find('.selected-value').text(filters[key].toString().replace(/\,/gi, ', '))
						}
						// deselect the only selection that the user made before
						else {
							delete filters[key];
							var resetLabel = $li.siblings('.reset').text();
							$li.siblings('li.reset').addClass('active');
							$li.siblings('li.reset').find('.selection-indicator').removeClass('snf-checkbox-unchecked').addClass('snf-checkbox-checked');
							$(this).closest(filterEl).find('.selected-value').text(resetLabel)

						}
					}
				}
			}
			$(tableDomID).dataTable().api().ajax.reload();
		});
	};

	function arrayFilter(filters, key, value, removeItem) {
		var prefix = 'sSearch_';
		if(!removeItem) {
			for(var prop in filters) {
				if(prop === key) {
						filters[prop].push(value);
				}
			}
		}
		else {
			if(filters[key].lenght === 1) { // to be checked
				delete filters[key];
			}
			else {
				var index = filters[key].indexOf(value);
				filters[key].splice(index, 1);
			}
		}
	};

	function textFilter(extraSearch) {
		snf.timer = 0;
		var $input = $(extraSearch).find('input');

		$input.keyup(function(e) {
			// if enter or space is pressed do nothing
			if(e.which !== 32 && e.which !== 13) {
				var key, value;
				key = $(this).data('filter');
				value = $.trim($(this).val());

				filters[key] = value;
				if (filters[key] === '') {
					delete filters[key];
				}
				if(snf.timer === 0) {
					snf.timer = 1;
					setTimeout(function() {
						$(tableDomID).dataTable().api().ajax.reload();
						snf.timer = 0;
					}, snf.ajaxdelay)
				}
			}
		})
	};

	textFilter('.filter-text');
	dropdownSelect('.filters .filter-dropdown .dropdown');


	/* Change Filters' View */

	$('.search-mode input').click(function(e) {
		e.stopPropagation();
		var $visFilters = $(this).closest('.search-mode').siblings('.filter:not(invisible)');
		var $invisFilters = $(this).closest('.search-mode').siblings('.filter.invisible');

		$visFilters.addClass('invisible').hide();
		if($invisFilters.hasClass('compact-view')) {
			$invisFilters.fadeIn(300).css('display', 'inline');
		}
		else {
			$invisFilters.fadeIn(300).css('display', 'inline-block');
		}
		$invisFilters.each(function() {
			$(this).removeClass('invisible');
		});
		$(this).closest('.search-mode').siblings('.filter:not(.invisible)').first().find('input').focus();
		if(!$('.compact-view').hasClass('invisible')) {
			standardToCompact();
		}
	});

	/* Tranfer the search terms of standard view to compact view */
	function standardToCompact() {
		var $advFilt = $('.filters').find('input[data-filter=compact-view]');
		var updated = true;
		hideFilterError();
		$advFilt.val(filtersToString());
	};

	function filtersToString() {
		var text = '';
		var newTerm;
		for(var prop in filters) {
			if(filtersInfo[prop] === 'text') {
				newTerm = prop + ': ' + filters[prop];
				if(text.length == 0) {
					text = newTerm;
				}
				else {
					text = text + ' ' + newTerm;
				}
			}
			else {
				newTerm = prop + ': ' + filters[prop].toString();
				if(text.length === 0) {
					text = newTerm;
				}
				else {
					text = text + ' ' + newTerm;
				}
			}
		}

		return text;
	};
	/* Compact View Functionality */

	$('.filters .compact-view').keyup(function(e) {
		if(e.which === 13) {
			$('.exec-search').trigger('click');
		}
	});

	$('.filters .toggle-instructions').click(function (e) {
		e.preventDefault();
		var that = this;
		$(this).find('.arrow').toggleClass('snf-angle-up snf-angle-down');
		if(!$(this).hasClass('open')) {
			$(this).addClass('open')
		}
		$(this).siblings('.content').stop().slideToggle(function() {
			if($(that).hasClass('open') && $(this).css('display') === 'none') {
				$(that).removeClass('open');
			}
		});
	})

	var filtersInfo = {};
	var tempFilters = {};
	var filtersResetValue = {};
	var filtersValidValues = {};

	/* Extract keys and values for filtersInfo, filtersResetValue, filtersValidValues the standard view */
	$('.filters').find('.filter:not(.compact-view)').each(function(index) {
		var key = $(this).find('*[data-filter]').attr('data-filter');
		var type; // possible values: 'singe-choice', 'multi-choice', 'text'
		var resetValue;
		if($(this).find('*[data-filter]').hasClass('dropdown')) {
			type = ($(this).closest('.filter-dropdown').hasClass('filter-boolean')? 'single-choice' : 'multi-choice');
			resetValue = $(this).find('li.reset').text().toUpperCase();
			filtersResetValue[key] = resetValue;
			filtersValidValues[key] = [];
			$(this).find('li:not(.divider)').each(function() {
				filtersValidValues[key].push($(this).text().toUpperCase());
			});
		}
		else {
			type = 'text';
		}
		filtersInfo[key] = type;
	});

	$('.exec-search').click(function(e) {
		e.preventDefault();
		tempFilters = {};
		var text = $(this).siblings('.form-group').find('input').val().trim();
		hideFilterError();
		if(text.length > 0) {
			var terms = text.split(' ');
			var key = 'unknown', value;
			var termsL = terms.length;
			var keyIndex;
			var lastkey;
			var filterType;
			var isKey = false;
			for(var i=0; i<termsL; i++) {
				terms[i] = terms[i].trim();
				for(var prop in filtersInfo) {
					if(terms[i].substring(0, prop.length+1).toUpperCase() === prop.toUpperCase() + ':') {
						key = prop;
						value = terms[i].substring(prop.length + 1).trim();
						isKey = true;
						break;
					}
				}
				if(!isKey) {
					value = terms[i];
				}

				if(!tempFilters[key]) {
					tempFilters[key] = value;
				}
				else if(value.length > 0) {
					tempFilters[key] = tempFilters[key] + ' ' + value;
				}
				isKey = false;
			}
		}

		if(!_.isEmpty(tempFilters)) {
			for(var filter in tempFilters) {
				for(var prop in filtersInfo) {
					if(prop === filter && (filtersInfo[prop] === 'single-choice' || filtersInfo[prop] === 'multi-choice')) {
						tempFilters[filter] = tempFilters[filter].replace(/\s*,\s*/g ,',').split(',');
						break;
					}
				}
			}
		}
		compactToStandard();
	});

	function compactToStandard() {
		var $filters = $('.filters');
		var $choicesLi;
		var valuesL;
		var validValues = [];
		var valid = true;
		var temp;
		if(_.isEmpty(tempFilters) && !_.isEmpty(filters)) {
			filters = {};
			$(tableDomID).dataTable().api().ajax.reload();
		}
		else {
			if(tempFilters['unknown']) {
				showFilterError(tempFilters['unknown']);
				valid = false;
			}
			for(var prop in tempFilters) {
				if(prop !== 'unknown') {
					temp = checkValues(prop);
					if(valid) {
						valid = temp;
					}
				}
			}
		}
		// execution
		if(valid) {
			resetBasicFilters();
			execFiltering();
		}
	};

	function checkValues(key) {
		var wrongTerm;
		var isWrong = false;
		if(filtersInfo[key] === 'text') {
			if(tempFilters[key] === '') {
				isWrong = true;
			}
		}
		else if(!isWrong) {
			var valuesUpperCased = $.map(tempFilters[key], function(item, index) {
				return item.toUpperCase();
			});
			var valuesL = valuesUpperCased.length;
			for(var i=0; i<valuesL; i++) {
				if(filtersValidValues[key].indexOf(valuesUpperCased[i]) === -1) {
					isWrong = true;
					break;
				}
			}
			if(!isWrong) {
				if(valuesUpperCased.indexOf(filtersResetValue[key])!==-1 && tempFilters[key].length>1) {
					isWrong = true;
				}
				else if(filtersInfo[key] === 'single-choice' && tempFilters[key].length > 1) {
					isWrong = true;
				}
			}
		}
		if(isWrong) {
			wrongTerm = key + ': ' + tempFilters[key].toString();
			showFilterError(wrongTerm);
			delete tempFilters[key];
		}
		return !isWrong;
	};

	function execFiltering() {
		var $choicesLi, valuesL;
		var $filters = $('.filters')
		for(var prop in tempFilters) {
			if(prop !== 'unknown') {
				if(filtersInfo[prop] === 'text'){
					$filters.find('input[data-filter="' + prop + '"]').val(tempFilters[prop]);
					$filters.find('input[data-filter="' + prop + '"]').trigger('keyup');
				}
				else {
					$choicesLi = $filters.find('*[data-filter="' + prop + '"] .choices').find('li');
					valuesL = tempFilters[prop].length;
					for(var i=0; i<valuesL; i++) { // for each filter
						$choicesLi.each(function() {
							if(tempFilters[prop][i].toUpperCase() === $(this).text().toUpperCase()) {
								if(!$(this).hasClass('active'))	{
									$(this).find('a').trigger('click');
								}
							}
						});
					}
				}
			}
		}
	};

	function showFilterError(wrongTerm) {
		var msg, addition, prevMsg;
		$errorDescr = $('.compact-view').find('.error-description');
		$errorSign = $('.compact-view').find('.error-sign');
		if($errorDescr.text() === '') {
			msg = 'Invalid search: "' + wrongTerm + '" is not valid.';
		}
		else {
			prevMsg = $errorDescr.text();
			addition =  ', "' + wrongTerm + '" are not valid.';
			msg = prevMsg.replace('term:', 'terms:');
			msg = msg.replace(' are not valid.', addition);
			msg = msg.replace(' is not valid.', addition);
		}
		$errorDescr.text(msg);
		$errorSign.css('opacity', 1)
	};

	function hideFilterError() {
		$('.compact-view').find('.error-sign').css('opacity', 0);
		$('.compact-view').find('.error-description').text('');
	};

	function resetBasicFilters() {
		$('.filters .filter-dropdown:not(.filter-boolean) li:not(.reset)').each(function() {
			if($(this).hasClass('active')) {
				$(this).find('a').trigger('click');
			}
		});
		$('.filters .filter-dropdown:not(.filter-boolean) li.reset').each(function() {
			if(!$(this).hasClass('active')) {
				$(this).find('a').trigger('click');
			}
		});

		$('.filters .filter-dropdown.filter-boolean li.reset').each(function() {
			if(!$(this).hasClass('active')) {
				$(this).find('a').trigger('click');
			}
		});

		$('.filters .filter-text').find('input').each(function() {
			if($(this).val().length !== 0) {
				$(this).val('');
				$(this).trigger('keyup')
			}
		});
	};



});
