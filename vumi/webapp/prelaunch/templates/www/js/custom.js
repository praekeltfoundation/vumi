$(document).ready(function() {
	
	/*--HEADER SIGNIN FLYOUT --*/
	$('#top-nav-signin').click(function() {
		$('#signin').slideToggle(1000,'easeOutElastic');
	});
	$('#forgot-click').click(function() {
		$('#signin-form').hide();
		$('#forgot-form').fadeIn();
	});
	$('#signin-return-click').click(function() {
		$('#forgot-form').hide();
		$('#signin-form').fadeIn();
	});
	$('#reset-again').click(function() {
		$('#signin').slideDown();
		$('#signin-form').hide();
		$('#forgot-form').fadeIn();
	});
	$('.go-signin').click(function() {
		$('#signin').slideToggle();
	});
	
	$(function() {
		$('#signin-form').validate();
		$('#forgot-form').validate();
		$('#signup').validate();
		$("#set-new-password").validate({
		  rules: {
			password: "required",
			confirm: {
			  equalTo: "#new-input-password"
			}
		  }
		});
	});
	
	/* -- SIGNUP VALIDATIONS -- */
	
	//Error messages show/hide
	jQuery(function () {
		//quote message
		jQuery('form#signup input[name="submit"]').click(function() {
  			if(jQuery('form#signup').has('input.error, select.error, textarea.error')){
			$('#signup-box').css("background-position","0 -300px");
			} else {
			alert("noerrors");
			}
		});
		//turn labels red
		jQuery('form').submit(function() {
			//turn labels red
			jQuery('input.error, select.error, textarea.error').parent().find('label').addClass('errored');
		});
		//remove labels red
		jQuery('select, input, textarea').change(function() {
			if(jQuery(this).parent().find('select, input').has('.valid')){
				jQuery(this).parent().find('label').removeClass('errored');
			}
		});
	});
	
	/*--APPLY NICE STYLE FOR SIGNIN INPUT WITH VALUES--*/	
	$('input.txtbox').blur(function() {
		if ($(this).val() != '') {
			$(this).addClass('typed');
		}
		else {
			$(this).removeClass('typed');
		}
	});
	
	$().ready(function() {
  		$('#feature-box').jqm({ajax: 'http://prototypes.praekelt.co.za/vumi/web/feature.html', trigger: 'a.jqModal'});
	});
	
});	