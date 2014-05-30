$(document).ready(function(){ $("input").focus(); })

$(document).ready(function(){

 // table sorting

   /*$('.table-sorted:not(.table-users)').tablesorter({
      sortList : [[2,0]],
  });*/

 // fix sub nav on scroll
  var $win = $(window)
    , $nav = $('.subnav:not(.main-nav)')
    , navTop = $('.subnav:not(.main-nav)').length && $('.subnav:not(.main-nav)').offset().top
    , isFixed = 0,
      navMainTop = $('.main-nav').outerHeight();

  function processScroll() {
    var i, scrollTop = $win.scrollTop();
    var navTemp = navTop - navMainTop*2;
    if (scrollTop >= (navTop - navMainTop*2) && !isFixed) {
      console.log('1 scrollTop: '+scrollTop+' navTop: '+navTemp);
      isFixed = 1;
      $nav.addClass('subnav-fixed');
    } else if (scrollTop <= (navTop -navMainTop*2) && isFixed) {
      console.log('2 scrollTop: '+scrollTop+' navTop: '+navTemp);
      isFixed = 0;
      $nav.removeClass('subnav-fixed');
    }
  }

  processScroll();

  // hack sad times - holdover until rewrite for 2.1
  $nav.on('click', function () {
    if (!isFixed) setTimeout(function () {  $win.scrollTop($win.scrollTop()) }, 10)
  })

  $win.on('scroll', processScroll)

  // hide/show expand/collapse 
  
  $('.subnav .dropdown-menu a').click(function(){
    $('.info-block-content, .show-hide-all').show();
  });

  var txt_all = ['+ Expand all','- Collapse all'];
  

  $('.show-hide-all span').text(txt_all[0]);
  
  
  $('.show-hide-all').click(function(e){
    e.preventDefault();
    $(this).toggleClass('open');
    var tabs = $(this).parent('.info-block').find('.object-details-content');


    if ($(this).hasClass('open')){
      $(this).text( txt_all[1]);
      tabs.each(function() {
        $(this).slideDown('slow');
        $(this).siblings('h4').addClass('expanded');
      });


    } else {
      $(this).text( txt_all[0]);
      tabs.each(function() {
        $(this).slideUp('slow');
        $(this).siblings('h4').removeClass('expanded');
      });
    }
  });   

 
  
  $('.object-details h4').click(function(){

    $(this).siblings('.object-details-content').toggle('slow');
    $(this).toggleClass('expanded');
  });


  $('.info-block h3').click(function(){
    $(this).next('.info-block-content').toggle('slow');
    $(this).prev('.show-hide-all').toggle('slow');
  });  
  
/*  $('.search-query').typeahead({
>>>>>>> admin: Basic initialization of table
    source: function(typeahead, query) {
      if (query.indexOf("@") > -1) {
        $.ajax({
          url:'/admin/api/users/?prefix='+query, 
          dataType:'json', 
          success: function(d){
            return typeahead.process(d);
          }
      })
      } else {
      }
    }
  })*/
})

